%% ----------------------------------------------------------------------------
%%
%% hanoidb: LSM-trees (Log-Structured Merge Trees) Indexed Storage
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

-module(hanoidb_level).
-author('Kresten Krab Thorup <krab@trifork.com>').

-include("include/plain_rpc.hrl").

-include("include/hanoidb.hrl").
-include("src/hanoidb.hrl").

%%
%% Manages 0..2 of hanoidb index file, and governs all aspects of
%% merging, lookup, folding, etc. for these files
%%

%%
%% DESIGN: uses plain_fsm, because we *want* selective receive to
%% postpone pending injects while we're merging.  That's a lot simpler
%% than maintaining to corresponding merge queue.
%%

-behavior(plain_fsm).
-export([data_vsn/0, code_change/3]).

-export([open/5, lookup/2, lookup/3, inject/2, close/1, snapshot_range/3, blocking_range/3,
         begin_incremental_merge/2, await_incremental_merge/1, set_max_level/2,
         unmerged_count/1, destroy/1]).

-include_lib("kernel/include/file.hrl").

-record(state, {
          a, b, c, next, dir, level, inject_done_ref, merge_pid, folding = [],
          step_next_ref, step_caller, step_merge_ref,
          opts = [], owner, work_in_progress=0, work_done=0, max_level=?TOP_LEVEL
          }).


%% if no activity for this long, we'll do some incremental
%% merge, just to clean up and optimize future operations.
-define(MERGE_TIMEOUT,1000).

-ifdef(DEBUG).
-define(log(Fmt,Args), debug_log(State,Fmt,Args)).
debug_log(State,Fmt,Args) ->
    Files = [if State#state.c == undefined -> $ ; true -> $C end,
             if State#state.b == undefined -> $ ; true -> $B end,
             if State#state.a == undefined -> $ ; true -> $A end ],
    io:format(user,"~p ~s~p[~s]: " ++ Fmt, [self(),
                                            if State#state.level < 10 -> "0"; true -> "" end,
                                            State#state.level,
                                            Files] ++ Args),
    ok.
-else.
-define(log(Fmt,Args),ok).
-endif.

%%%%% PUBLIC OPERATIONS

open(Dir,Level,Next,Opts,Owner) when Level>0 ->
    hanoidb_util:ensure_expiry(Opts),
    SpawnOpt = hanoidb:get_opt(spawn_opt, Opts, []),
    PID = plain_fsm:spawn_opt(?MODULE,
                              fun() ->
                                      process_flag(trap_exit,true),
                                      link(Owner),
                                      initialize(#state{dir=Dir,level=Level,next=Next,opts=Opts,owner=Owner})
                              end,
                              SpawnOpt),
    {ok, PID}.

lookup(Ref, Key) ->
    plain_rpc:call(Ref, {lookup, Key}).

lookup(Ref, Key, ReplyFun) ->
    plain_rpc:cast(Ref, {lookup, Key, ReplyFun}).

inject(Ref, FileName) ->
    Result = plain_rpc:call(Ref, {inject, FileName}),
    Result.

begin_incremental_merge(Ref, StepSize) ->
    plain_rpc:call(Ref, {begin_incremental_merge, StepSize}).

await_incremental_merge(Ref) ->
    plain_rpc:call(Ref, await_incremental_merge).

unmerged_count(Ref) ->
    plain_rpc:call(Ref, unmerged_count).

set_max_level(Ref, LevelNo) ->
    plain_rpc:cast(Ref, {set_max_level, LevelNo}).

close(Ref) ->
    try
        plain_rpc:call(Ref, close)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        exit:{normal, _} -> ok
    end.

destroy(Ref) ->
    try
        plain_rpc:call(Ref, destroy)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        exit:{normal, _} -> ok
    end.

snapshot_range(Ref, FoldWorkerPID, Range) ->
    {ok, Folders} = plain_rpc:call(Ref, {init_snapshot_range_fold, FoldWorkerPID, Range, []}),
    FoldWorkerPID ! {initialize, Folders},
    ok.

blocking_range(Ref, FoldWorkerPID, Range) ->
    {ok, Folders} = plain_rpc:call(Ref, {init_blocking_range_fold, FoldWorkerPID, Range, []}),
    FoldWorkerPID ! {initialize, Folders},
    ok.

%%%%% INTERNAL

data_vsn() ->
    5.

code_change(_OldVsn, _State, _Extra) ->
    {ok, {#state{}, data_vsn()}}.


initialize(State) ->
    hanoidb_util:ensure_expiry(State#state.opts),

    try
        _Result = initialize2(State),
        ?log(" ** terminated ~p", [_Result])
    catch
        Class:Ex when not (Class == exit andalso Ex == normal) ->
            ?log("crashing ~p:~p ~p~n", [Class,Ex,erlang:get_stacktrace()]),
            error_logger:error_msg("crash: ~p:~p ~p~n", [Class,Ex,erlang:get_stacktrace()])
    end.

initialize2(State) ->
%    error_logger:info_msg("in ~p level=~p~n", [self(), State]),

    AFileName = filename("A",State),
    BFileName = filename("B",State),
    CFileName = filename("C",State),
    MFileName = filename("M",State),

    %% remove old merge file
    file:delete(filename("X",State)),

    %% remove old fold files (hard links to A/B/C used during fold)
    file:delete(filename("AF",State)),
    file:delete(filename("BF",State)),
    file:delete(filename("CF",State)),

    case file:read_file_info(MFileName) of
        {ok, _} ->

            %% Recover from post-merge crash. This is the case where
            %% a merge completed, resulting in a file that needs to
            %% stay at the *same* level because the resulting size
            %% is smaller than or equal to this level's files.
            file:delete(AFileName),
            file:delete(BFileName),
            ok = file:rename(MFileName, AFileName),

            {ok, IXA} = hanoidb_reader:open(AFileName, [random|State#state.opts]),

            case file:read_file_info(CFileName) of
                {ok, _} ->
                    file:rename(CFileName, BFileName),
                    {ok, IXB} = hanoidb_reader:open(BFileName, [random|State#state.opts]),
                    check_begin_merge_then_loop0(init_state(State#state{ a= IXA, b=IXB }));

                {error, enoent} ->
                    main_loop(init_state(State#state{ a= IXA, b=undefined }))
            end;

        {error, enoent} ->
            case file:read_file_info(BFileName) of
                {ok, _} ->
                    {ok, IXA} = hanoidb_reader:open(AFileName, [random|State#state.opts]),
                    {ok, IXB} = hanoidb_reader:open(BFileName, [random|State#state.opts]),

                    IXC =
                        case file:read_file_info(CFileName) of
                            {ok, _} ->
                                {ok, C} = hanoidb_reader:open(CFileName, [random|State#state.opts]),
                                C;
                        {error, enoent} ->
                            undefined
                    end,

                    check_begin_merge_then_loop0(init_state(State#state{ a=IXA, b=IXB, c=IXC }));

                {error, enoent} ->

                    %% assert that there is no C file
                    {error, enoent} = file:read_file_info(CFileName),

                    case file:read_file_info(AFileName) of
                        {ok, _} ->
                            {ok, IXA} = hanoidb_reader:open(AFileName, [random|State#state.opts]),
                            main_loop(init_state(State#state{ a=IXA }));

                        {error, enoent} ->
                            main_loop(init_state(State))
                    end
            end
    end.

init_state(State) ->
    ?log("opened level ~p, state=~p", [State#state.level, State]),
    State.

check_begin_merge_then_loop0(State=#state{a=IXA, b=IXB, merge_pid=undefined})
  when IXA/=undefined, IXB /= undefined ->
    {ok, MergePID} = begin_merge(State),
    MergeRef = monitor(process, MergePID),
    WIP =
        if State#state.c == undefined ->
                ?BTREE_SIZE(State#state.level);
           true ->
                2*?BTREE_SIZE(State#state.level)
        end,

    MergePID ! {step, {self(), MergeRef}, WIP},
    main_loop(State#state{merge_pid=MergePID,work_done=0,
                          work_in_progress=WIP,step_merge_ref=MergeRef});

check_begin_merge_then_loop0(State) ->
    check_begin_merge_then_loop(State).


check_begin_merge_then_loop(State=#state{a=IXA, b=IXB, merge_pid=undefined})
  when IXA/=undefined, IXB /= undefined ->
    {ok, MergePID} = begin_merge(State),
    main_loop(State#state{merge_pid=MergePID,work_done=0 });
check_begin_merge_then_loop(State) ->
    main_loop(State).

main_loop(State = #state{ next=Next }) ->
    Parent = plain_fsm:info(parent),
    receive
        ?CALL(From, {lookup, Key})=Req ->
            case do_lookup(Key, [State#state.c, State#state.b, State#state.a, Next]) of
                not_found ->
                    plain_rpc:send_reply(From, not_found);
                {found, Result} ->
                    plain_rpc:send_reply(From, {ok, Result});
                {delegate, DelegatePid} ->
                    DelegatePid ! Req
            end,
            main_loop(State);

        ?CAST(_From, {lookup, Key, ReplyFun})=Req ->
            case do_lookup(Key, [State#state.c, State#state.b, State#state.a, Next]) of
                not_found ->
                    ReplyFun(not_found);
                {found, Result} ->
                    ReplyFun({ok, Result});
                {delegate, DelegatePid} ->
                    DelegatePid ! Req
            end,
            main_loop(State);

        ?CALL(From, {inject, FileName}) when State#state.c == undefined ->

            {ToFileName, SetPos} =
                case {State#state.a, State#state.b} of
                    {undefined, undefined} ->
                        {filename("A",State), #state.a};
                    {_, undefined} ->
                        {filename("B",State), #state.b};
                    {_, _} ->
                        {filename("C",State), #state.c}
                end,

            ?log("inject ~s~n", [ToFileName]),

            case file:rename(FileName, ToFileName) of
                ok -> ok;
                E  -> ?log("rename failed ~p -> ~p :: ~p~n", [FileName, ToFileName, E]),
                      error(E)
            end,

            plain_rpc:send_reply(From, ok),

            case hanoidb_reader:open(ToFileName, [random|State#state.opts]) of
                {ok, BT} ->
                    if SetPos == #state.b ->
                            check_begin_merge_then_loop(setelement(SetPos, State, BT));
                       true ->
                            main_loop(setelement(SetPos, State, BT))
                    end;
                E2  -> ?log("open failed ~p :: ~p~n", [ToFileName, E2]),
                       error(E2)
            end;


        ?CALL(From, unmerged_count) ->
            plain_rpc:send_reply(From, total_unmerged(State)),
            main_loop(State);

        %% propagate knowledge of new max level
        ?CAST(_From, {set_max_level, Max}) ->
            if Next =/= undefined ->
                    set_max_level(Next, Max);
               true ->
                    ok
            end,
            main_loop(State#state{ max_level=Max });

        %% replies OK when there is no current step in progress
        ?CALL(From, {begin_incremental_merge, StepSize})
          when State#state.step_merge_ref == undefined,
               State#state.step_next_ref == undefined ->
            plain_rpc:send_reply(From, ok),
            do_step(undefined, 0, StepSize, State);

        ?CALL(From, await_incremental_merge)
          when State#state.step_merge_ref == undefined,
               State#state.step_next_ref == undefined ->
            plain_rpc:send_reply(From, ok),
            main_loop(State);

        %% accept step any time there is not an outstanding step
        ?CALL(StepFrom, {step_level, DoneWork, StepSize})
          when State#state.step_merge_ref  == undefined,
               State#state.step_caller     == undefined,
               State#state.step_next_ref   == undefined
               ->
            do_step(StepFrom, DoneWork, StepSize, State);

        {MRef, step_done} when MRef == State#state.step_merge_ref ->
            demonitor(MRef, [flush]),

            ?log("step_done", []),

            State1 = State#state{ work_done = State#state.work_done + State#state.work_in_progress,
                                  work_in_progress = 0 },

            State2 =
                case State1#state.step_next_ref of
                    undefined ->
                        reply_step_ok(State1);
                    _ ->
                        State1
                end,

            main_loop(State2#state{ step_merge_ref=undefined });

        {_MRef, step_done}=Msg ->
            ?log("unexpected step_done", []),
            exit({bad_msg, Msg});

        {'DOWN', MRef, _, _, _Reason} when MRef == State#state.step_merge_ref ->

            ?log("merge worker died ~p", [_Reason]),
            %% current merge worker died (or just finished)

            State2 =
                case State#state.step_next_ref of
                    undefined ->
                        reply_step_ok(State);
                    _ ->
                        State
                end,

            main_loop(State2#state{ step_merge_ref=undefined, work_in_progress=0 });


        ?REPLY(MRef, step_ok)
          when MRef =:= State#state.step_next_ref,
               State#state.step_merge_ref =:= undefined ->

            ?log("got step_ok", []),
            %% this applies when we receive an OK from the next level,
            %% and we have finished the incremental merge at this level

            State2 = reply_step_ok(State),
            main_loop(State2#state{ step_next_ref=undefined });

        ?CALL(From, close) ->
            close_if_defined(State#state.a),
            close_if_defined(State#state.b),
            close_if_defined(State#state.c),
            [stop_if_defined(PID) || PID <- [State#state.merge_pid | State#state.folding]],

            %% this is synchronous all the way down, because our
            %% caller is monitoring *this* proces, and thus the
            %% rpc would fail when we fall off the cliff
            if Next == undefined -> ok;
               true ->
                    hanoidb_level:close(Next)
            end,
            plain_rpc:send_reply(From, ok),
            {ok, closing};

        ?CALL(From, destroy) ->
            destroy_if_defined(State#state.a),
            destroy_if_defined(State#state.b),
            destroy_if_defined(State#state.c),
            [stop_if_defined(PID) || PID <- [State#state.merge_pid | State#state.folding]],

            %% this is synchronous all the way down, because our
            %% caller is monitoring *this* proces, and thus the
            %% rpc would fail when we fall off the cliff
            if Next == undefined -> ok;
               true ->
                    hanoidb_level:destroy(Next)
            end,
            plain_rpc:send_reply(From, ok),
            {ok, destroying};

        ?CALL(From, {init_snapshot_range_fold, WorkerPID, Range, List}) when State#state.folding == [] ->

            ?log("init_range_fold ~p -> ~p", [Range, WorkerPID]),

            {NextList, FoldingPIDs} =
            case {State#state.a, State#state.b, State#state.c} of
                {undefined, undefined, undefined} ->
                    {List, []};

                {_, undefined, undefined} ->
                    ok = file:make_link(filename("A", State), filename("AF", State)),
                    {ok, PID0} = start_range_fold(filename("AF",State), WorkerPID, Range, State),
                    {[PID0|List], [PID0]};

                {_, _, undefined} ->
                    ok = file:make_link(filename("A", State), filename("AF", State)),
                    {ok, PIDA} = start_range_fold(filename("AF",State), WorkerPID, Range, State),

                    ok = file:make_link(filename("B", State), filename("BF", State)),
                    {ok, PIDB} = start_range_fold(filename("BF",State), WorkerPID, Range, State),

                    {[PIDA,PIDB|List], [PIDB,PIDA]};

                {_, _, _} ->
                    ok = file:make_link(filename("A", State), filename("AF", State)),
                    {ok, PIDA} = start_range_fold(filename("AF",State), WorkerPID, Range, State),

                    ok = file:make_link(filename("B", State), filename("BF", State)),
                    {ok, PIDB} = start_range_fold(filename("BF",State), WorkerPID, Range, State),

                    ok = file:make_link(filename("C", State), filename("CF", State)),
                    {ok, PIDC} = start_range_fold(filename("CF",State), WorkerPID, Range, State),

                    {[PIDA,PIDB,PIDC|List], [PIDC,PIDB,PIDA]}
            end,

            case Next of
                undefined ->
                    plain_rpc:send_reply(From, {ok, lists:reverse(NextList)});
                _ ->
                    Next ! ?CALL(From, {init_snapshot_range_fold, WorkerPID, Range, NextList})
            end,

            main_loop(State#state{ folding = FoldingPIDs });

        {range_fold_done, PID, FoldFileName} ->
            ok = file:delete(FoldFileName),
            NewFolding = lists:delete(PID,State#state.folding),
            main_loop(State#state{ folding = NewFolding });

        ?CALL(From, {init_blocking_range_fold, WorkerPID, Range, List}) ->

            RefList =
                case {State#state.a, State#state.b, State#state.c} of
                    {undefined, undefined, undefined} ->
                        List;

                    {_, undefined, undefined} ->
                        ARef = erlang:make_ref(),
                        ok = do_range_fold(State#state.a, WorkerPID, ARef, Range),
                        [ARef|List];

                    {_, _, undefined} ->
                        BRef = erlang:make_ref(),
                        ok = do_range_fold(State#state.b, WorkerPID, BRef, Range),

                        ARef = erlang:make_ref(),
                        ok = do_range_fold(State#state.a, WorkerPID, ARef, Range),

                        [ARef,BRef|List];

                    {_, _, _} ->
                        CRef = erlang:make_ref(),
                        ok = do_range_fold(State#state.c, WorkerPID, CRef, Range),

                        BRef = erlang:make_ref(),
                        ok = do_range_fold(State#state.b, WorkerPID, BRef, Range),

                        ARef = erlang:make_ref(),
                        ok = do_range_fold(State#state.a, WorkerPID, ARef, Range),

                        [ARef,BRef,CRef|List]
                end,

            case Next of
                undefined ->
                    plain_rpc:send_reply(From, {ok, lists:reverse(RefList)});
                _ ->
                    Next ! ?CALL(From, {init_blocking_range_fold, WorkerPID, Range, RefList})
            end,

            main_loop(State);


        %%
        %% The outcome of merging resulted in a file with less than
        %% level #entries, so we keep it at this level
        %%
        ?CAST(_From,{merge_done, 0, OutFileName}) ->
            ok = file:delete(OutFileName),
            {ok, State2} = close_and_delete_a_and_b(State),
            case State#state.c of
                undefined ->
                    main_loop(State2#state{ merge_pid=undefined });
                CFile ->
                    ok = hanoidb_reader:close(CFile),
                    ok = file:rename(filename("C", State2), filename("A", State2)),
                    {ok, AFile} = hanoidb_reader:open(filename("A", State2), [random|State#state.opts]),
                    main_loop(State2#state{ a = AFile, c = undefined, merge_pid=undefined })
            end;

        ?CAST(_From,{merge_done, Count, OutFileName})
          when Count =< ?BTREE_SIZE(State#state.level),
               State#state.c =:= undefined,
               Next =:= undefined ->

            ?log("merge_done, out:~w~n -> self", [Count]),

            % first, rename the tmp file to M, so recovery will pick it up
            MFileName = filename("M",State),
            ok = file:rename(OutFileName, MFileName),

            % then delete A and B (if we crash now, C will become the A file)
            {ok, State2} = close_and_delete_a_and_b(State),

            % then, rename M to A, and open it
            AFileName = filename("A",State2),
            ok = file:rename(MFileName, AFileName),
            {ok, AFile} = hanoidb_reader:open(AFileName, [random|State#state.opts]),

            % iff there is a C file, then move it to B position
            % TODO: consider recovery for this
            case State#state.c of
                undefined ->
                    main_loop(State2#state{ a=AFile, b=undefined, merge_pid=undefined });
                CFile ->
                    ok = hanoidb_reader:close(CFile),
                    ok = file:rename(filename("C", State2), filename("B", State2)),
                    {ok, BFile} = hanoidb_reader:open(filename("B", State2), [random|State#state.opts]),
                    check_begin_merge_then_loop(State2#state{ a=AFile, b=BFile, c=undefined,
                                                              merge_pid=undefined })
            end;

        %%
        %% We need to push the output of merging to the next level
        %%
        ?CAST(_,{merge_done, _Count, OutFileName}) ->

            ?log("merge_done, out:~w, next:~p~n", [_Count,Next]),

            State1 =
                if Next =:= undefined ->
                        {ok, PID} = ?MODULE:open(State#state.dir, State#state.level + 1, undefined,
                                                 State#state.opts, State#state.owner ),
                        State#state.owner ! { bottom_level, State#state.level + 1 },
                        State#state{ next=PID, max_level= State#state.level+1 };
                   true ->
                        State
                end,

            %% no need to rename it since we don't accept new injects

            MRef = plain_rpc:send_call(State1#state.next, {inject, OutFileName}),
            main_loop(State1#state{ inject_done_ref = MRef, merge_pid=undefined });

        %%
        %% Our successor accepted the inject
        %%
        ?REPLY(MRef, ok) when MRef == State#state.inject_done_ref ->
            erlang:demonitor(MRef, [flush]),

            {ok, State2} = close_and_delete_a_and_b(State),

            % if there is a "C" file, then move it to "A" position.
            State3 =
                case State2#state.c of
                    undefined ->
                        State2;
                    TreeFile ->
                        %% TODO: on what OS's is it ok to rename an open file?
                        ok = file:rename(filename("C", State2), filename("A", State2)),
                        State2#state{ a = TreeFile, b=undefined, c=undefined }
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
            ?log("*** merge_died: ~p~n", [Reason]),
            restart_merge_then_loop(State#state{merge_pid=undefined}, Reason);
        {'EXIT', PID, _} when PID == hd(State#state.folding);
                              PID == hd(tl(State#state.folding));
                              PID == hd(tl(tl(State#state.folding)))
                              ->
            main_loop(State#state{ folding = lists:delete(PID,State#state.folding) });
        {'EXIT', PID, Reason} ->
            ?log("got unexpected exit ~p from ~p~n", [Reason, PID]),
            error_logger:info_msg("got unexpected exit ~p from ~p~n", [Reason, PID])

    end.

do_step(StepFrom, PreviousWork, StepSize, State) ->
    WorkLeftHere =
        if (State#state.b =/= undefined) andalso (State#state.merge_pid =/= undefined) ->
                max(0, (2 * ?BTREE_SIZE(State#state.level)) - State#state.work_done);
           true ->
                 0
        end,
    WorkUnit      = StepSize,
    MaxLevel      = max(State#state.max_level, State#state.level),
    Depth         = MaxLevel-?TOP_LEVEL+1,
    TotalWork     = Depth * WorkUnit,
    WorkUnitsLeft = max(0, TotalWork-PreviousWork),

    %% TODO: The goal was to offer a less variable latency option for merging but this
    %% heuristic doesn't work when there are aggressive deletes (expiry or delete).
    %% https://github.com/basho/hanoidb/issues/7
    WorkToDoHere =
        %% min(WorkLeftHere, WorkUnitsLeft),
        case hanoidb:get_opt( merge_strategy, State#state.opts, fast) of
            fast ->
                min(WorkLeftHere, WorkUnitsLeft);
            predictable ->
                if (WorkLeftHere < Depth * WorkUnit) ->
                        min(WorkLeftHere, WorkUnit);
                   true ->
                        min(WorkLeftHere, WorkUnitsLeft)
                end
        end,
    WorkIncludingHere  = PreviousWork + WorkToDoHere,

    ?log("do_step prev:~p, do:~p of ~p ~n", [PreviousWork, WorkToDoHere, WorkLeftHere]),

    %% delegate the step_level request to the next level
    Next = State#state.next,
    DelegateRef =
        if Next =:= undefined ->
                undefined;
           true ->
                plain_rpc:send_call(Next, {step_level, WorkIncludingHere, StepSize})
    end,

    MergeRef =
        if WorkToDoHere > 0 ->
                MergePID = State#state.merge_pid,
                Monitor = monitor(process, MergePID),
                MergePID ! {step, {self(), Monitor}, WorkToDoHere},
                Monitor;
           true ->
                undefined
        end,

    if (DelegateRef =:= undefined) andalso (MergeRef =:= undefined) ->
            %% nothing to do ... just return OK

            State2 = reply_step_ok(State#state { step_caller = StepFrom }),
            main_loop(State2);
       true ->
            main_loop(State#state{ step_next_ref    = DelegateRef,
                                   step_caller      = StepFrom,
                                   step_merge_ref   = MergeRef,
                                   work_in_progress = WorkToDoHere
                                 })
    end.

reply_step_ok(State) ->
    case State#state.step_caller of
        undefined ->
            ok;
        _ ->
            ?log("step_ok -> ~p", [State#state.step_caller]),
            plain_rpc:send_reply(State#state.step_caller, step_ok)
    end,
    State#state{ step_caller=undefined }.

total_unmerged(State) ->
    Files =
          (if State#state.b == undefined -> 0; true -> 2 end)
%        + (if State#state.c == undefined -> 0; true -> 1 end)
,

    Files * 1 * ?BTREE_SIZE( State#state.level ).



do_lookup(_Key, []) ->
    not_found;
do_lookup(_Key, [Pid]) when is_pid(Pid) ->
    {delegate, Pid};
do_lookup(Key, [undefined|Rest]) ->
    do_lookup(Key, Rest);
do_lookup(Key, [BT|Rest]) ->
    case hanoidb_reader:lookup(BT, Key) of
        {ok, ?TOMBSTONE} ->
            not_found;
        {ok, Result} ->
            {found, Result};
        not_found ->
            do_lookup(Key, Rest)
    end.

close_if_defined(undefined) -> ok;
close_if_defined(BT)        -> hanoidb_reader:close(BT).

destroy_if_defined(undefined) -> ok;
destroy_if_defined(BT)        -> hanoidb_reader:destroy(BT).

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

    ?log("starting merge~n", []),

    file:delete(XFileName),

    MergePID = proc_lib:spawn_link(fun() ->
         try
                       ?log("merge begun~n", []),

                       {ok, OutCount} = hanoidb_merger:merge(AFileName, BFileName, XFileName,
                                                           ?BTREE_SIZE(State#state.level + 1),
                                                           State#state.next =:= undefined,
                                                           State#state.opts ),

                       Owner ! ?CAST(self(),{merge_done, OutCount, XFileName})
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

    ok = hanoidb_reader:close(State#state.a),
    ok = hanoidb_reader:close(State#state.b),

    ok = file:delete(AFileName),
    ok = file:delete(BFileName),

    {ok, State#state{a=undefined, b=undefined}}.


filename(PFX, State) ->
    filename:join(State#state.dir, PFX ++ "-" ++ integer_to_list(State#state.level) ++ ".data").


start_range_fold(FileName, WorkerPID, Range, State) ->
    Owner = self(),
    PID = proc_lib:spawn( fun() ->
          try
              ?log("start_range_fold ~p on ~p -> ~p", [self(), FileName, WorkerPID]),
              erlang:link(WorkerPID),
              {ok, File} = hanoidb_reader:open(FileName, [folding|State#state.opts]),
              do_range_fold2(File, WorkerPID, self(), Range),
              erlang:unlink(WorkerPID),
              hanoidb_reader:close(File),

              %% this will release the pinning of the fold file
              Owner  ! {range_fold_done, self(), FileName},
              ok
          catch
    Class:Ex ->
        io:format(user, "BAD: ~p:~p ~p~n", [Class,Ex,erlang:get_stacktrace()])
          end
                          end ),
    {ok, PID}.

-spec do_range_fold(BT        :: hanoidb_reader:read_file(),
                    WorkerPID :: pid(),
                    SelfOrRef :: pid() | reference(),
                    Range     :: #key_range{} ) -> ok.
do_range_fold(BT, WorkerPID, SelfOrRef, Range) ->
    case hanoidb_reader:range_fold(fun(Key,Value,_) ->
                                             WorkerPID ! {level_result, SelfOrRef, Key, Value},
                                             ok
                                     end,
                                     ok,
                                     BT,
                                     Range) of
        {limit, _, LastKey} ->
            WorkerPID ! {level_limit, SelfOrRef, LastKey},
            ok;
        {done, _} ->
            %% tell fold merge worker we're done
            WorkerPID ! {level_done, SelfOrRef},
            ok
    end.

-define(FOLD_CHUNK_SIZE, 100).

-spec do_range_fold2(BT        :: hanoidb_reader:read_file(),
                    WorkerPID :: pid(),
                    SelfOrRef :: pid() | reference(),
                    Range     :: #key_range{} ) -> ok.
do_range_fold2(BT, WorkerPID, SelfOrRef, Range) ->
    try hanoidb_reader:range_fold(fun(Key,Value,{0,KVs}) ->
                                         send(WorkerPID, SelfOrRef, [{Key,Value}|KVs]),
                                         {?FOLD_CHUNK_SIZE-1, []};
                                    (Key,Value,{N,KVs}) ->
                                         {N-1,[{Key,Value}|KVs]}
                                 end,
                                 {?FOLD_CHUNK_SIZE-1,[]},
                                 BT,
                                 Range) of
        {limit, {_,KVs}, LastKey} ->
            send(WorkerPID, SelfOrRef, KVs),
            WorkerPID ! {level_limit, SelfOrRef, LastKey};
        {done, {_, KVs}} ->
            %% tell fold merge worker we're done
            send(WorkerPID, SelfOrRef, KVs),
            WorkerPID ! {level_done, SelfOrRef}
    catch
        exit:worker_died -> ok
    end,
    ok.

send(_,_,[]) ->
    [];
send(WorkerPID,Ref,ReverseKVs) ->
    try
        plain_rpc:call(WorkerPID, {level_results, Ref, lists:reverse(ReverseKVs)})
    catch
        %% the fold worker died; just ignore it
        exit:normal   -> exit(worker_died);
        exit:shutdown -> exit(worker_died);
        exit:noproc   -> exit(worker_died)
    end.
