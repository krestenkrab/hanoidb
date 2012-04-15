%% ----------------------------------------------------------------------------
%%
%% lsm_btree: LSM-trees (Log-Structured Merge Trees) Indexed Storage
%%
%% Copyright 2011-2012 (c) Trifork A/S.  All Rights Reserved.
%% http://trifork.com/ info@trifork.com
%%
%% Copyright 2012 (c) Basho Technologies, Inc.  All Rights Reserved.
%% http://basho.com/ info@basho.com
%%
%% This file is provided to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
%% License for the specific language governing permissions and limitations
%% under the License.
%%
%% ----------------------------------------------------------------------------

-module(lsm_btree_level).
-author('Kresten Krab Thorup <krab@trifork.com>').

-include("lsm_btree.hrl").

%%
%% Manages a "pair" of lsm_index (or rathern, 0, 1 or 2), and governs
%% the process of injecting/merging parent trees into this pair.
%%

%%
%% For now, we use plain_fsm, because we *want* selective receive to postpone
%% pending injects while we're merging.  That's a lot simpler than maintaining
%% to corresponding merge queue.
%%

-behavior(plain_fsm).
-export([data_vsn/0, code_change/3]).

-export([open/3, lookup/2, inject/2, close/1, async_range/3, sync_range/3]).

-include_lib("kernel/include/file.hrl").

-record(state, {
          a, b, next, dir, level, inject_done_ref, merge_pid, folding = []
          }).

%%%%% PUBLIC OPERATIONS

open(Dir,Level,Next) when Level>0 ->
    {ok, plain_fsm:spawn_link(?MODULE,
                              fun() ->
                                      process_flag(trap_exit,true),
                                      initialize(#state{dir=Dir,level=Level,next=Next})
                              end)}.

lookup(Ref, Key) ->
    call(Ref, {lookup, Key}).

inject(Ref, FileName) ->
    Result = call(Ref, {inject, FileName}),
    Result.

close(Ref) ->
    try
        call(Ref, close)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok
    end.



async_range(Ref, FoldWorkerPID, Range) ->
    proc_lib:spawn(fun() ->
                           {ok, Folders} = call(Ref, {init_range_fold, FoldWorkerPID, Range, []}),
                           FoldWorkerPID ! {initialize, Folders}
                   end),
    {ok, FoldWorkerPID}.

sync_range(Ref, FoldWorkerPID, Range) ->
    {ok, Folders} = call(Ref, {sync_range_fold, FoldWorkerPID, Range, []}),
    FoldWorkerPID ! {initialize, Folders},
    {ok, FoldWorkerPID}.

%%%%% INTERNAL

data_vsn() ->
    5.

code_change(_OldVsn, _State, _Extra) ->
    {ok, {#state{}, data_vsn()}}.


-define(REQ(From,Msg), {'$req', From, Msg}).
-define(REPLY(Ref,Msg), {'$rep', Ref, Msg}).

send_request(PID, Request) ->
    Ref = erlang:monitor(process, PID),
    PID ! ?REQ({self(), Ref}, Request),
    Ref.

receive_reply(MRef) ->
    receive
        ?REPLY(MRef, Reply) ->
            erlang:demonitor(MRef, [flush]),
            Reply;
                {'DOWN', MRef, _, _, Reason} ->
                    exit(Reason)
    end.

call(PID,Request) ->
    Ref = send_request(PID, Request),
    receive_reply(Ref).

reply({PID,Ref}, Reply) ->
    erlang:send(PID, ?REPLY(Ref, Reply)),
    ok.


initialize(State) ->
%    error_logger:info_msg("in ~p level=~p~n", [self(), State]),

    AFileName = filename("A",State),
    BFileName = filename("B",State),
    CFileName = filename("C",State),

    %% remove old merge file
    file:delete( filename("X",State)),

    %% remove old fold files (hard links to A/B used during fold)
    file:delete( filename("AF",State)),
    file:delete( filename("BF",State)),

    case file:read_file_info(CFileName) of
        {ok, _} ->

            %% recover from post-merge crash
            file:delete(AFileName),
            file:delete(BFileName),
            ok = file:rename(CFileName, AFileName),

            {ok, BT} = lsm_btree_reader:open(CFileName, random),
            main_loop(State#state{ a= BT, b=undefined });

        {error, enoent} ->
            case file:read_file_info(BFileName) of
                {ok, _} ->
                    {ok, BT1} = lsm_btree_reader:open(AFileName, random),
                    {ok, BT2} = lsm_btree_reader:open(BFileName, random),

                    check_begin_merge_then_loop(State#state{ a=BT1, b=BT2 });

                {error, enoent} ->

                    case file:read_file_info(AFileName) of
                        {ok, _} ->
                            {ok, BT1} = lsm_btree_reader:open(AFileName, random),
                            main_loop(State#state{ a=BT1 });

                        {error, enoent} ->
                            main_loop(State)
                    end
            end
    end.

check_begin_merge_then_loop(State=#state{a=BT1, b=BT2, merge_pid=undefined})
  when BT1/=undefined, BT2 /= undefined ->
    {ok, MergePID} = begin_merge(State),
    main_loop(State#state{merge_pid=MergePID });
check_begin_merge_then_loop(State) ->
    main_loop(State).

main_loop(State = #state{ next=Next }) ->
    Parent = plain_fsm:info(parent),
    receive
        ?REQ(From, {lookup, Key})=Req ->
            case do_lookup(Key, [State#state.b, State#state.a, Next]) of
                not_found ->
                    reply(From, not_found);
                {found, Result} ->
                    reply(From, {ok, Result});
                {delegate, DelegatePid} ->
                    DelegatePid ! Req
            end,
            main_loop(State);

        ?REQ(From, {inject, FileName}) when State#state.b == undefined ->
            if State#state.a == undefined ->
                    ToFileName = filename("A",State),
                    SetPos = #state.a;
               true ->
                    ToFileName = filename("B",State),
                    SetPos = #state.b
            end,
            ok = file:rename(FileName, ToFileName),
            {ok, BT} = lsm_btree_reader:open(ToFileName, random),
            reply(From, ok),
            check_begin_merge_then_loop(setelement(SetPos, State, BT));

        ?REQ(From, close) ->
            close_if_defined(State#state.a),
            close_if_defined(State#state.b),
            stop_if_defined(State#state.merge_pid),
            reply(From, ok),

            %% this is synchronous all the way down, because our
            %% caller is monitoring *this* proces, and thus the
            %% rpc would fail when we fall off the cliff
            if Next == undefined -> ok;
               true ->
                    lsm_btree_level:close(Next)
            end,
            ok;

        ?REQ(From, {init_range_fold, WorkerPID, Range, List}) when State#state.folding == [] ->

            case {State#state.a, State#state.b} of
                {undefined, undefined} ->
                    NewFolding = [],
                    NextList = List;

                {_, undefined} ->
                    ok = file:make_link(filename("A", State), filename("AF", State)),
                    {ok, PID0} = start_range_fold(filename("AF",State), WorkerPID, Range),
                    NextList = [PID0|List],
                    NewFolding = [PID0];

                {_, _} ->
                    ok = file:make_link(filename("A", State), filename("AF", State)),
                    {ok, PID0} = start_range_fold(filename("AF",State), WorkerPID, Range),

                    ok = file:make_link(filename("B", State), filename("BF", State)),
                    {ok, PID1} = start_range_fold(filename("BF",State), WorkerPID, Range),

                    NextList = [PID1,PID0|List],
                    NewFolding = [PID1,PID0]
            end,

            case Next of
                undefined ->
                    reply(From, {ok, lists:reverse(NextList)});
                _ ->
                    Next ! ?REQ(From, {init_range_fold, WorkerPID, Range, NextList})
            end,

            main_loop(State#state{ folding = NewFolding });

        {range_fold_done, PID, [_,$F|_]=FoldFileName} ->
            ok = file:delete(FoldFileName),
            main_loop(State#state{ folding = lists:delete(PID,State#state.folding) });

        ?REQ(From, {sync_range_fold, WorkerPID, Range, List}) ->

            case {State#state.a, State#state.b} of
                {undefined, undefined} ->
                    RefList = List;

                {_, undefined} ->
                    ARef = erlang:make_ref(),
                    ok = do_range_fold(State#state.a, WorkerPID, ARef, Range),
                    RefList = [ARef|List];

                {_, _} ->
                    BRef = erlang:make_ref(),
                    ok = do_range_fold(State#state.b, WorkerPID, BRef, Range),

                    ARef = erlang:make_ref(),
                    ok = do_range_fold(State#state.a, WorkerPID, ARef, Range),

                    RefList = [ARef,BRef|List]
            end,

            case Next of
                undefined ->
                    reply(From, {ok, lists:reverse(RefList)});
                _ ->
                    Next ! ?REQ(From, {sync_range_fold, WorkerPID, Range, RefList})
            end,

            main_loop(State);


        %%
        %% The outcome of merging resulted in a file with less than
        %% level #entries, so we keep it at this level
        %%
        {merge_done, Count, OutFileName} when Count =< ?BTREE_SIZE(State#state.level) ->

            % first, rename the tmp file to C, so recovery will pick it up
            CFileName = filename("C",State),
            ok = file:rename(OutFileName, CFileName),

            % then delete A and B (if we crash now, C will become the A file)
            {ok, State2} = close_a_and_b(State),

            % then, rename C to A, and open it
            AFileName = filename("A",State2),
            ok = file:rename(CFileName, AFileName),
            {ok, BT} = lsm_btree_reader:open(AFileName, random),

            main_loop(State2#state{ a=BT, b=undefined, merge_pid=undefined });

        %%
        %% We need to push the output of merging to the next level
        %%
        {merge_done, _, OutFileName} ->
            State1 =
                if Next =:= undefined ->
                        {ok, PID} = ?MODULE:open(State#state.dir, State#state.level + 1, undefined),
                        State#state{ next=PID };
                   true ->
                        State
                end,

            MRef = send_request(State1#state.next, {inject, OutFileName}),
            main_loop(State1#state{ inject_done_ref = MRef, merge_pid=undefined });

        %%
        %% Our successor accepted the inject
        %%
        ?REPLY(MRef, ok) when MRef =:= State#state.inject_done_ref ->
            erlang:demonitor(MRef, [flush]),
            {ok, State2} = close_a_and_b(State),
            main_loop(State2#state{ inject_done_ref=undefined });

        %%
        %% Our successor died!
        %%
                {'DOWN', MRef, _, _, Reason} when MRef =:= State#state.inject_done_ref ->
                    exit(Reason);

        %% gen_fsm handling
        {system, From, Req} ->
            plain_fsm:handle_system_msg(
              From, Req, State, fun(S1) -> main_loop(S1) end);

        {'EXIT', Parent, Reason} ->
            plain_fsm:parent_EXIT(Reason, State);
        {'EXIT', _, normal} ->
            %% Probably from a merger_pid - which we may have forgotten in the meantime.
            main_loop(State);
        {'EXIT', Pid, Reason} when Pid == State#state.merge_pid ->
            restart_merge_then_loop(State#state{merge_pid=undefined}, Reason);
        {'EXIT', PID, _} when [PID] == tl(State#state.folding);
                              hd(State#state.folding) == PID ->
            main_loop(State#state{ folding = lists:delete(PID,State#state.folding) });
        {'EXIT', PID, Reason} ->
            error_logger:info_msg("got unexpected exit ~p from ~p~n", [Reason, PID])

    end.

do_lookup(_Key, []) ->
    not_found;
do_lookup(_Key, [Pid]) when is_pid(Pid) ->
    {delegate, Pid};
do_lookup(Key, [undefined|Rest]) ->
    do_lookup(Key, Rest);
do_lookup(Key, [BT|Rest]) ->
    case lsm_btree_reader:lookup(BT, Key) of
        {ok, ?TOMBSTONE} -> not_found;
        {ok, Result}  -> {found, Result};
        not_found     -> do_lookup(Key, Rest)
    end.

close_if_defined(undefined) -> ok;
close_if_defined(BT)        -> lsm_btree_reader:close(BT).

stop_if_defined(undefined) -> ok;
stop_if_defined(MergePid) when is_pid(MergePid) ->
    erlang:exit(MergePid, shutdown).


restart_merge_then_loop(State, Reason) ->
    XFileName = filename("X",State),
    error_logger:warning_msg("Merger appears to have failed (reason: ~p). Removing outfile ~s\n", [Reason, XFileName]),
    file:delete(XFileName),
    check_begin_merge_then_loop(State).

begin_merge(State) ->
    AFileName = filename("A",State),
    BFileName = filename("B",State),
    XFileName = filename("X",State),
    Owner = self(),

    file:delete(XFileName),

    MergePID = proc_lib:spawn_link(fun() ->
                       {ok, OutCount} = lsm_btree_merger:merge(AFileName, BFileName, XFileName,
                                                                   ?BTREE_SIZE(State#state.level + 1),
                                                                   State#state.next =:= undefined),
%                       error_logger:info_msg("merge done ~p,~p -> ~p~n", [AFileName, BFileName, XFileName]),

                       Owner ! {merge_done, OutCount, XFileName}
               end),

    {ok, MergePID}.


close_a_and_b(State) ->
    AFileName = filename("A",State),
    BFileName = filename("B",State),

    ok = lsm_btree_reader:close(State#state.a),
    ok = lsm_btree_reader:close(State#state.b),

    ok = file:delete(AFileName),
    ok = file:delete(BFileName),

    {ok, State#state{a=undefined, b=undefined}}.


filename(PFX, State) ->
    filename:join(State#state.dir, PFX ++ "-" ++ integer_to_list(State#state.level) ++ ".data").


start_range_fold(FileName, WorkerPID, Range) ->
    Owner = self(),
    PID =
        proc_lib:spawn( fun() ->
                                erlang:link(WorkerPID),
                                {ok, File} = lsm_btree_reader:open(FileName, sequential),
                                do_range_fold(File, WorkerPID, self(), Range),
                                erlang:unlink(WorkerPID),

                                %% this will release the pinning of the fold file
                                Owner  ! {range_fold_done, self(), FileName}
                        end ),
    {ok, PID}.

do_range_fold(BT, WorkerPID, Self, Range) ->
    lsm_btree_reader:range_fold(fun(Key,Value,_) ->
                                        WorkerPID ! {level_result, Self, Key, Value},
                                        ok
                                end,
                                ok,
                                BT,
                                Range),

    %% tell fold merge worker we're done
    WorkerPID ! {level_done, Self},

    ok.
