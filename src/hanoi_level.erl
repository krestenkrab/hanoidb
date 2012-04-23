%% ----------------------------------------------------------------------------
%%
%% hanoi: LSM-trees (Log-Structured Merge Trees) Indexed Storage
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

-module(hanoi_level).
-author('Kresten Krab Thorup <krab@trifork.com>').

-include("include/hanoi.hrl").
-include("src/hanoi.hrl").

%%
%% Manages a "pair" of hanoi index (or rathern, 0, 1 or 2), and governs
%% the process of injecting/merging parent trees into this pair.
%%

%%
%% For now, we use plain_fsm, because we *want* selective receive to postpone
%% pending injects while we're merging.  That's a lot simpler than maintaining
%% to corresponding merge queue.
%%

-behavior(plain_fsm).
-export([data_vsn/0, code_change/3]).

-export([open/5, lookup/2, inject/2, close/1, snapshot_range/3, blocking_range/3,
         incremental_merge/2, unmerged_count/1]).

-include_lib("kernel/include/file.hrl").

-record(state, {
          a, b, c, next, dir, level, inject_done_ref, merge_pid, folding = [],
          step_next_ref, step_caller, step_merge_ref,
          opts = [], owner, work_done=0
          }).

%%%%% PUBLIC OPERATIONS

open(Dir,Level,Next,Opts,Owner) when Level>0 ->
    PID = plain_fsm:spawn_link(?MODULE,
                              fun() ->
                                      process_flag(trap_exit,true),
                                      initialize(#state{dir=Dir,level=Level,next=Next,opts=Opts,owner=Owner})
                              end),
    {ok, PID}.

lookup(Ref, Key) ->
    call(Ref, {lookup, Key}).

inject(Ref, FileName) ->
    Result = call(Ref, {inject, FileName}),
    Result.

incremental_merge(Ref,HowMuch) ->
    call(Ref, {incremental_merge, HowMuch}).

unmerged_count(Ref) ->
    call(Ref, unmerged_count).

close(Ref) ->
    try
        call(Ref, close)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok
    end.



snapshot_range(Ref, FoldWorkerPID, Range) ->
    proc_lib:spawn(fun() ->
                           {ok, Folders} = call(Ref, {init_snapshot_range_fold, FoldWorkerPID, Range, []}),
                           FoldWorkerPID ! {initialize, Folders}
                   end),
    {ok, FoldWorkerPID}.

blocking_range(Ref, FoldWorkerPID, Range) ->
    {ok, Folders} = call(Ref, {init_blocking_range_fold, FoldWorkerPID, Range, []}),
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

    try
        initialize2(State)
    catch
        Class:Ex when not (Class == exit andalso Ex == normal) ->
            error_logger:error_msg("crash2: ~p:~p ~p~n", [Class,Ex,erlang:get_stacktrace()])
    end.

initialize2(State) ->
%    error_logger:info_msg("in ~p level=~p~n", [self(), State]),

    AFileName = filename("A",State),
    BFileName = filename("B",State),
    CFileName = filename("C",State),
    MFileName = filename("M",State),

    %% remove old merge file
    file:delete( filename("X",State)),

    %% remove old fold files (hard links to A/B/C used during fold)
    file:delete( filename("AF",State)),
    file:delete( filename("BF",State)),
    file:delete( filename("CF",State)),

    case file:read_file_info(MFileName) of
        {ok, _} ->

            %% recover from post-merge crash
            file:delete(AFileName),
            file:delete(BFileName),
            ok = file:rename(MFileName, AFileName),

            {ok, BT} = hanoi_reader:open(AFileName, random),

            case file:read_file_info(CFileName) of
                {ok, _} ->
                    file:rename(CFileName, BFileName),
                    {ok, BT2} = hanoi_reader:open(BFileName, random),
                    check_begin_merge_then_loop(State#state{ a= BT, b=BT2 });

                {error, enoent} ->
                    main_loop(State#state{ a= BT, b=undefined })
            end;

        {error, enoent} ->
            case file:read_file_info(BFileName) of
                {ok, _} ->
                    {ok, BT1} = hanoi_reader:open(AFileName, random),
                    {ok, BT2} = hanoi_reader:open(BFileName, random),

                    case file:read_file_info(CFileName) of
                        {ok, _} ->
                            {ok, BT3} = hanoi_reader:open(CFileName, random);
                        {error, enoent} ->
                            BT3 = undefined
                    end,

                    check_begin_merge_then_loop(State#state{ a=BT1, b=BT2, c=BT3 });

                {error, enoent} ->

                    case file:read_file_info(AFileName) of
                        {ok, _} ->
                            {ok, BT1} = hanoi_reader:open(AFileName, random),
                            main_loop(State#state{ a=BT1 });

                        {error, enoent} ->
                            main_loop(State)
                    end
            end
    end.

check_begin_merge_then_loop(State=#state{a=BT1, b=BT2, merge_pid=undefined})
  when BT1/=undefined, BT2 /= undefined ->
    {ok, MergePID} = begin_merge(State),
    main_loop(State#state{merge_pid=MergePID,work_done=0 });
check_begin_merge_then_loop(State) ->
    main_loop(State).

main_loop(State = #state{ next=Next }) ->
    Parent = plain_fsm:info(parent),
    receive
        ?REQ(From, {lookup, Key})=Req ->
            case do_lookup(Key, [State#state.c, State#state.b, State#state.a, Next]) of
                not_found ->
                    reply(From, not_found);
                {found, Result} ->
                    reply(From, {ok, Result});
                {delegate, DelegatePid} ->
                    DelegatePid ! Req
            end,
            main_loop(State);

        ?REQ(From, {inject, FileName}) when State#state.c == undefined ->
            case {State#state.a, State#state.b} of
                {undefined, undefined} ->
                    ToFileName = filename("A",State),
                    SetPos = #state.a;
                {_, undefined} ->
                    ToFileName = filename("B",State),
                    SetPos = #state.b;
                {_, _} ->
                    ToFileName = filename("C",State),
                    SetPos = #state.c
            end,
            ok = file:rename(FileName, ToFileName),
            {ok, BT} = hanoi_reader:open(ToFileName, random),

            reply(From, ok),
            check_begin_merge_then_loop(setelement(SetPos, State, BT));


        ?REQ(From, unmerged_count) ->
            reply(From, total_unmerged(State)),
            main_loop(State);

        %% replies OK when there is no current step in progress
        ?REQ(From, {incremental_merge, HowMuch})
          when State#state.step_merge_ref == undefined,
               State#state.step_next_ref == undefined ->
            reply(From, ok),
            if HowMuch > 0 ->
                    self() ! ?REQ(undefined, {step, HowMuch});
               true ->
                    ok
            end,
            main_loop(State);

        %% accept step any time there is not an outstanding step
        ?REQ(StepFrom, {step, HowMuch})
          when State#state.step_merge_ref == undefined,
               State#state.step_caller    == undefined,
               State#state.step_next_ref  == undefined
               ->

            WorkLeftHere = max(0,total_unmerged(State) - State#state.work_done),
            WorkToDoHere = min(WorkLeftHere, HowMuch),
            DelegateWork = max(0,HowMuch - WorkToDoHere),

            %% delegate the step request to the next level
            if Next =:= undefined; DelegateWork == 0 ->
                    DelegateRef = undefined;
               true ->
                    DelegateRef = send_request(Next, {step, DelegateWork})
            end,

            if (State#state.merge_pid == undefined)
               orelse (WorkToDoHere =< 0) ->
                    MergeRef = undefined,
                    NewWorkDone = State#state.work_done;
               true ->
                    MergePID = State#state.merge_pid,
                    MergeRef = monitor(process, MergePID),
                    MergePID ! {step, {self(), MergeRef}, WorkToDoHere},
                    NewWorkDone = State#state.work_done + WorkToDoHere
            end,

            if (Next =:= undefined) andalso (MergeRef =:= undefined) ->
                    %% nothing to do ... just return OK
                    State2 = reply_step_ok(State#state { step_caller = StepFrom }),
                    main_loop(State2#state { work_done = NewWorkDone });
               true ->
                    main_loop(State#state{ step_next_ref=DelegateRef,
                                           step_caller=StepFrom,
                                           step_merge_ref=MergeRef,
                                           work_done = NewWorkDone
                                         })
            end;

        {MRef, step_done} when MRef == State#state.step_merge_ref ->
            demonitor(MRef, [flush]),

            case State#state.step_next_ref of
                undefined ->
                    State2 = reply_step_ok(State);
                _ ->
                    State2 = State
            end,

            main_loop(State2#state{ step_merge_ref=undefined });

        {'DOWN', MRef, _, _, _} when MRef == State#state.step_merge_ref ->

            %% current merge worker died (or just finished)

            case State#state.step_next_ref of
                undefined ->
                    State2 = reply_step_ok(State);
                _ ->
                    State2 = State
            end,

            main_loop(State2#state{ step_merge_ref=undefined });


        ?REPLY(MRef, ok)
          when MRef =:= State#state.step_next_ref,
               State#state.step_merge_ref =:= undefined ->

            %% this applies when we receive an OK from the next level,
            %% and we have finished the incremental merge at this level

            State2 = reply_step_ok(State),
            main_loop(State2#state{ step_next_ref=undefined });

        ?REQ(From, close) ->
            close_if_defined(State#state.a),
            close_if_defined(State#state.b),
            close_if_defined(State#state.c),
            stop_if_defined(State#state.merge_pid),
            reply(From, ok),

            %% this is synchronous all the way down, because our
            %% caller is monitoring *this* proces, and thus the
            %% rpc would fail when we fall off the cliff
            if Next == undefined -> ok;
               true ->
                    hanoi_level:close(Next)
            end,
            ok;

        ?REQ(From, {init_snapshot_range_fold, WorkerPID, Range, List}) when State#state.folding == [] ->

            case {State#state.a, State#state.b, State#state.c} of
                {undefined, undefined, undefined} ->
                    FoldingPIDs = [],
                    NextList = List;

                {_, undefined, undefined} ->
                    ok = file:make_link(filename("A", State), filename("AF", State)),
                    {ok, PID0} = start_range_fold(filename("AF",State), WorkerPID, Range),
                    NextList = [PID0|List],
                    FoldingPIDs = [PID0];

                {_, _, undefined} ->
                    ok = file:make_link(filename("A", State), filename("AF", State)),
                    {ok, PIDA} = start_range_fold(filename("AF",State), WorkerPID, Range),

                    ok = file:make_link(filename("B", State), filename("BF", State)),
                    {ok, PIDB} = start_range_fold(filename("BF",State), WorkerPID, Range),

                    NextList = [PIDA,PIDB|List],
                    FoldingPIDs = [PIDB,PIDA];

                {_, _, _} ->
                    ok = file:make_link(filename("A", State), filename("AF", State)),
                    {ok, PIDA} = start_range_fold(filename("AF",State), WorkerPID, Range),

                    ok = file:make_link(filename("B", State), filename("BF", State)),
                    {ok, PIDB} = start_range_fold(filename("BF",State), WorkerPID, Range),

                    ok = file:make_link(filename("C", State), filename("CF", State)),
                    {ok, PIDC} = start_range_fold(filename("CF",State), WorkerPID, Range),

                    NextList = [PIDA,PIDB,PIDC|List],
                    FoldingPIDs = [PIDC,PIDB,PIDA]
            end,

            case Next of
                undefined ->
                    reply(From, {ok, lists:reverse(NextList)});
                _ ->
                    Next ! ?REQ(From, {init_snapshot_range_fold, WorkerPID, Range, NextList})
            end,

            main_loop(State#state{ folding = FoldingPIDs });

        {range_fold_done, PID, FoldFileName} ->
            ok = file:delete(FoldFileName),
            NewFolding = lists:delete(PID,State#state.folding),
            main_loop(State#state{ folding = NewFolding });

        ?REQ(From, {init_blocking_range_fold, WorkerPID, Range, List}) ->

            case {State#state.a, State#state.b, State#state.c} of
                {undefined, undefined, undefined} ->
                    RefList = List;

                {_, undefined, undefined} ->
                    ARef = erlang:make_ref(),
                    ok = do_range_fold(State#state.a, WorkerPID, ARef, Range),
                    RefList = [ARef|List];

                {_, _, undefined} ->
                    BRef = erlang:make_ref(),
                    ok = do_range_fold(State#state.b, WorkerPID, BRef, Range),

                    ARef = erlang:make_ref(),
                    ok = do_range_fold(State#state.a, WorkerPID, ARef, Range),

                    RefList = [ARef,BRef|List];

                {_, _, _} ->
                    CRef = erlang:make_ref(),
                    ok = do_range_fold(State#state.c, WorkerPID, CRef, Range),

                    BRef = erlang:make_ref(),
                    ok = do_range_fold(State#state.b, WorkerPID, BRef, Range),

                    ARef = erlang:make_ref(),
                    ok = do_range_fold(State#state.a, WorkerPID, ARef, Range),

                    RefList = [ARef,BRef,CRef|List]
            end,

            case Next of
                undefined ->
                    reply(From, {ok, lists:reverse(RefList)});
                _ ->
                    Next ! ?REQ(From, {init_blocking_range_fold, WorkerPID, Range, RefList})
            end,

            main_loop(State);


        %%
        %% The outcome of merging resulted in a file with less than
        %% level #entries, so we keep it at this level
        %%
        {merge_done, Count, OutFileName} when Count =< ?BTREE_SIZE(State#state.level) ->

            % first, rename the tmp file to M, so recovery will pick it up
            MFileName = filename("M",State),
            ok = file:rename(OutFileName, MFileName),

            % then delete A and B (if we crash now, C will become the A file)
            {ok, State2} = close_and_delete_a_and_b(State),

            % then, rename M to A, and open it
            AFileName = filename("A",State2),
            ok = file:rename(MFileName, AFileName),
            {ok, BT} = hanoi_reader:open(AFileName, random),

            % iff there is a C file, then move it to B position
            % TODO: consider recovery for this
            case State#state.c of
                undefined ->
                    main_loop(State2#state{ a=BT, b=undefined, merge_pid=undefined });
                TreeFile ->
                    file:rename(filename("C",State2), filename("B", State2)),
                    check_begin_merge_then_loop(State2#state{ a=BT, b=TreeFile, c=undefined,
                                                              merge_pid=undefined })

            end;

        %%
        %% We need to push the output of merging to the next level
        %%
        {merge_done, _, OutFileName} ->
            State1 =
                if Next =:= undefined ->
                        {ok, PID} = ?MODULE:open(State#state.dir, State#state.level + 1, undefined,
                                                 State#state.opts, State#state.owner ),
                        State#state.owner ! { bottom_level, State#state.level + 1 },
                        State#state{ next=PID };
                   true ->
                        State
                end,

            %% no need to rename it since we don't accept new injects

            MRef = send_request(State1#state.next, {inject, OutFileName}),
            main_loop(State1#state{ inject_done_ref = MRef, merge_pid=undefined });

        %%
        %% Our successor accepted the inject
        %%
        ?REPLY(MRef, ok) when MRef =:= State#state.inject_done_ref ->
            erlang:demonitor(MRef, [flush]),
            {ok, State2} = close_and_delete_a_and_b(State),

            % if there is a "C" file, then move it to "A" position.
            case State2#state.c of
                undefined ->
                    State3=State2;
                TreeFile ->
                    %% TODO: on what OS's is it ok to rename an open file?
                    ok = file:rename(filename("C", State2), filename("A", State2)),
                    State3 = State2#state{ a = TreeFile, b=undefined, c=undefined }
            end,

            main_loop(State3#state{ inject_done_ref=undefined });

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

reply_step_ok(State) ->
    case State#state.step_caller of
        undefined -> ok;
        _ -> reply(State#state.step_caller, ok)
    end,
    State#state{ step_caller=undefined }.

total_unmerged(State) ->
    Files =
          (if State#state.b == undefined -> 0; true -> 1 end)
        + (if State#state.c == undefined -> 0; true -> 1 end),

    Files * ?BTREE_SIZE( State#state.level ).



do_lookup(_Key, []) ->
    not_found;
do_lookup(_Key, [Pid]) when is_pid(Pid) ->
    {delegate, Pid};
do_lookup(Key, [undefined|Rest]) ->
    do_lookup(Key, Rest);
do_lookup(Key, [BT|Rest]) ->
    case hanoi_reader:lookup(BT, Key) of
        {ok, ?TOMBSTONE} ->
            not_found;
        {ok, Result} ->
            {found, Result};
        not_found ->
            do_lookup(Key, Rest)
    end.

close_if_defined(undefined) -> ok;
close_if_defined(BT)        -> hanoi_reader:close(BT).

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
         try
                       {ok, OutCount} = hanoi_merger:merge(AFileName, BFileName, XFileName,
                                                           ?BTREE_SIZE(State#state.level + 1),
                                                           State#state.next =:= undefined,
                                                           State#state.opts ),
%                       error_logger:info_msg("merge done ~p,~p -> ~p~n", [AFileName, BFileName, XFileName]),

                       Owner ! {merge_done, OutCount, XFileName}
         catch
            C:E ->
                 error_logger:error_msg("merge failed ~p:~p ~p~n",
                                        [C,E,erlang:get_stacktrace()]),
                 erlang:raise(C,E,erlang:get_stacktrace())
         end
               end),

    {ok, MergePID}.


close_and_delete_a_and_b(State) ->
    AFileName = filename("A",State),
    BFileName = filename("B",State),

    ok = hanoi_reader:close(State#state.a),
    ok = hanoi_reader:close(State#state.b),

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
                                {ok, File} = hanoi_reader:open(FileName, sequential),
                                do_range_fold(File, WorkerPID, self(), Range),
                                erlang:unlink(WorkerPID),
                                hanoi_reader:close(File),

                                %% this will release the pinning of the fold file
                                Owner  ! {range_fold_done, self(), FileName}
                        end ),
    {ok, PID}.

-spec do_range_fold(BT        :: hanoi_reader:read_file(),
                    WorkerPID :: pid(),
                    SelfOrRef :: pid() | reference(),
                    Range     :: #btree_range{} ) -> ok.
do_range_fold(BT, WorkerPID, SelfOrRef, Range) ->
    case hanoi_reader:range_fold(fun(Key,Value,_) ->
                                             WorkerPID ! {level_result, SelfOrRef, Key, Value},
                                             ok
                                     end,
                                     ok,
                                     BT,
                                     Range) of
        {limit, _, LastKey} ->
            WorkerPID ! {level_limit, SelfOrRef, LastKey};
        {done, _} ->
            %% tell fold merge worker we're done
            WorkerPID ! {level_done, SelfOrRef}

    end,
    ok.
