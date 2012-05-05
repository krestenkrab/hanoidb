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

-module(hanoi).
-author('Kresten Krab Thorup <krab@trifork.com>').


-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([open/1, open/2, transact/2, close/1, get/2, lookup/2, delete/2, put/3,
         fold/3, fold_range/4, destroy/1]).

-export([get_opt/2, get_opt/3]).

-include("hanoi.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("include/hanoi.hrl").

-record(state, { top, nursery, dir, opt, max_level }).

%% PUBLIC API

-type hanoi() :: pid().
-type key_range() :: #btree_range{}.

% @doc
% Create or open existing hanoi store.  Argument `Dir' names a
% directory in which to keep the data files.  By convention, we
% name hanoi data directories with extension ".hanoi".
% @spec open(Dir::string()) -> pid().
- spec open(Dir::string()) -> pid().
open(Dir) ->
    open(Dir, []).

- spec open(Dir::string(), Opts::[_]) -> pid().
open(Dir, Opts) ->
    ok = start_app(),
    gen_server:start(?MODULE, [Dir, Opts], []).

% @doc
% Close a Hanoi data store.
% @spec close(Ref::pid()) -> ok
- spec close(Ref::pid()) -> ok.
close(Ref) ->
    try
        gen_server:call(Ref, close, infinity)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %% Handle the case where the monitor triggers
        exit:{normal, _} -> ok
    end.

-spec destroy(Ref::pid()) -> ok.
destroy(Ref) ->
    try
        gen_server:call(Ref, destroy, infinity)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %% Handle the case where the monitor triggers
        exit:{normal, _} -> ok
    end.

get(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {get, Key}, infinity).

%% for compatibility with original code
lookup(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {get, Key}, infinity).

-spec delete(hanoi(), binary()) ->
                    ok | {error, term()}.
delete(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {delete, Key}, infinity).

-spec put(hanoi(), binary(), binary()) ->
                 ok | {error, term()}.
put(Ref,Key,Value) when is_binary(Key), is_binary(Value) ->
    gen_server:call(Ref, {put, Key, Value}, infinity).

-type transact_spec() :: {put, binary(), binary()} | {delete, binary()}.
-spec transact(hanoi(), [transact_spec()]) ->
                 ok | {error, term()}.
transact(Ref, TransactionSpec) ->
    gen_server:call(Ref, {transact, TransactionSpec}, infinity).

-type kv_fold_fun() ::  fun((binary(),binary(),any())->any()).

-spec fold(hanoi(),kv_fold_fun(),any()) -> any().
fold(Ref,Fun,Acc0) ->
    fold_range(Ref,Fun,Acc0,#btree_range{from_key= <<>>, to_key=undefined}).

-spec fold_range(hanoi(),kv_fold_fun(),any(),key_range()) -> any().
fold_range(Ref,Fun,Acc0,Range) ->
    if Range#btree_range.limit < 10 ->
            {ok, PID} = gen_server:call(Ref, {blocking_range, self(), Range}, infinity);
       true ->
            {ok, PID} = gen_server:call(Ref, {snapshot_range, self(), Range}, infinity)
    end,
    MRef = erlang:monitor(process, PID),
    receive_fold_range(MRef, PID,Fun,Acc0).

receive_fold_range(MRef, PID,Fun,Acc0) ->
    receive

        %% receive one K/V from fold_worker
        {fold_result, PID, K,V} ->
            case
                try
                    {ok, Fun(K,V,Acc0)}
                catch
                    Class:Exception ->
                        % io:format(user, "Exception in hanoi fold: ~p ~p", [Exception, erlang:get_stacktrace()]),
                        % lager:warn("Exception in hanoi fold: ~p", [Exception]),
                        {'EXIT', Class, Exception, erlang:get_stacktrace()}
                end
            of
                {ok, Acc1} ->
                    receive_fold_range(MRef, PID, Fun, Acc1);
                Exit ->
                    %% kill the fold worker ...
                    PID ! die,
                    drain_worker_and_throw(MRef,PID,Exit)
            end;

        %% receive multiple KVs from fold_worker
        {fold_results, PID, KVs} ->
            case
                try
                    {ok, kvfoldl(Fun,Acc0,KVs)}
                catch
                    Class:Exception ->
                        lager:warning("Exception in hanoi fold: ~p", [Exception]),
                        {'EXIT', Class, Exception, erlang:get_stacktrace()}
                end
            of
                {ok, Acc1} ->
                    receive_fold_range(MRef, PID, Fun, Acc1);
                Exit ->
                    %% kill the fold worker ...
                    erlang:exit(PID, kill),
                    drain_worker_and_throw(MRef,PID,Exit)
            end;
        {fold_limit, PID, _} ->
            erlang:demonitor(MRef, [flush]),
            Acc0;
        {fold_done, PID} ->
            erlang:demonitor(MRef, [flush]),
            Acc0;
        {'DOWN', MRef, _, _, Reason} ->
            error({fold_worker_died, Reason})
    end.

kvfoldl(_Fun,Acc0,[]) ->
    Acc0;
kvfoldl(Fun,Acc0,[{K,V}|T]) ->
    kvfoldl(Fun, Fun(K,V,Acc0), T).

raise({'EXIT', Class, Exception, Trace}) ->
    erlang:raise(Class, Exception, Trace).

drain_worker_and_throw(MRef, PID, ExitTuple) ->
    receive
        {fold_result, PID, _, _} ->
            drain_worker_and_throw(MRef, PID, ExitTuple);
        {fold_results, PID, _} ->
            drain_worker_and_throw(MRef, PID, ExitTuple);
        {'DOWN', MRef, _, _, _} ->
            raise(ExitTuple);
        {fold_limit, PID, _} ->
            erlang:demonitor(MRef, [flush]),
            raise(ExitTuple);
        {fold_done, PID} ->
            erlang:demonitor(MRef, [flush]),
            raise(ExitTuple)
    end.


init([Dir, Opts]) ->
    case file:read_file_info(Dir) of
        {ok, #file_info{ type=directory }} ->
            {ok, TopLevel, MaxLevel} = open_levels(Dir,Opts),
            {ok, Nursery} = hanoi_nursery:recover(Dir, TopLevel, MaxLevel);

        {error, E} when E =:= enoent ->
            ok = file:make_dir(Dir),
            {ok, TopLevel} = hanoi_level:open(Dir, ?TOP_LEVEL, undefined, Opts, self()),
            MaxLevel = ?TOP_LEVEL,
            {ok, Nursery} = hanoi_nursery:new(Dir, MaxLevel)
    end,

    {ok, #state{ top=TopLevel, dir=Dir, nursery=Nursery, opt=Opts, max_level=MaxLevel }}.



open_levels(Dir,Options) ->
    {ok, Files} = file:list_dir(Dir),

    %% parse file names and find max level
    {MinLevel,MaxLevel} =
        lists:foldl(fun(FileName, {MinLevel,MaxLevel}) ->
                            case parse_level(FileName) of
                                {ok, Level} ->
                                    { erlang:min(MinLevel, Level),
                                      erlang:max(MaxLevel, Level) };
                                _ ->
                                    {MinLevel,MaxLevel}
                            end
                    end,
                    {?TOP_LEVEL, ?TOP_LEVEL},
                    Files),

%    error_logger:info_msg("found level files ... {~p,~p}~n", [MinLevel, MaxLevel]),

    %% remove old nursery file
    file:delete(filename:join(Dir,"nursery.data")),

    %%
    %% Do enough incremental merge to be sure we won't deadlock in insert
    %%
    {TopLevel, MaxMerge} =
        lists:foldl( fun(LevelNo, {NextLevel, MergeWork0}) ->
                             {ok, Level} = hanoi_level:open(Dir,LevelNo,NextLevel,Options,self()),

                             MergeWork = MergeWork0 + hanoi_level:unmerged_count(Level),

                             {Level, MergeWork}
                     end,
                     {undefined, 0},
                     lists:seq(MaxLevel, min(?TOP_LEVEL, MinLevel), -1)),

    WorkPerIter = (MaxLevel-MinLevel+1)*?BTREE_SIZE(?TOP_LEVEL),
    do_merge(TopLevel, WorkPerIter, MaxMerge),

    {ok, TopLevel, MaxLevel}.

do_merge(TopLevel, _Inc, N) when N =< 0 ->
    ok = hanoi_level:await_incremental_merge(TopLevel);

do_merge(TopLevel, Inc, N) ->
    ok = hanoi_level:begin_incremental_merge(TopLevel),
    do_merge(TopLevel, Inc, N-Inc).



parse_level(FileName) ->
    case re:run(FileName, "^[^\\d]+-(\\d+)\\.data$", [{capture,all_but_first,list}]) of
        {match,[StringVal]} ->
            {ok, list_to_integer(StringVal)};
        _ ->
            nomatch
    end.


handle_info({bottom_level, N}, #state{ nursery=Nursery, top=TopLevel }=State)
  when N > State#state.max_level ->
    State2 = State#state{ max_level = N,
                          nursery= hanoi_nursery:set_max_level(Nursery, N) },

    hanoi_level:set_max_level(TopLevel, N),

    {noreply, State2};

handle_info(Info,State) ->
    error_logger:error_msg("Unknown info ~p~n", [Info]),
    {stop,bad_msg,State}.

handle_cast(Info,State) ->
    error_logger:error_msg("Unknown cast ~p~n", [Info]),
    {stop,bad_msg,State}.


%% premature delete -> cleanup
terminate(normal,_State) ->
    ok;
terminate(_Reason,_State) ->
    error_logger:info_msg("got terminate(~p,~p)~n", [_Reason,_State]),
    % flush_nursery(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



handle_call({snapshot_range, Sender, Range}, _From, State=#state{ top=TopLevel, nursery=Nursery }) ->
    {ok, FoldWorkerPID} = hanoi_fold_worker:start(Sender),
    hanoi_nursery:do_level_fold(Nursery, FoldWorkerPID, Range),
    Result = hanoi_level:snapshot_range(TopLevel, FoldWorkerPID, Range),
    {reply, Result, State};

handle_call({blocking_range, Sender, Range}, _From, State=#state{ top=TopLevel, nursery=Nursery }) ->
    {ok, FoldWorkerPID} = hanoi_fold_worker:start(Sender),
    hanoi_nursery:do_level_fold(Nursery, FoldWorkerPID, Range),
    Result = hanoi_level:blocking_range(TopLevel, FoldWorkerPID, Range),
    {reply, Result, State};

handle_call({put, Key, Value}, _From, State) when is_binary(Key), is_binary(Value) ->
    {ok, State2} = do_put(Key, Value, State),
    {reply, ok, State2};

handle_call({transact, TransactionSpec}, _From, State) ->
    {ok, State2} = do_transact(TransactionSpec, State),
    {reply, ok, State2};

handle_call({delete, Key}, _From, State) when is_binary(Key) ->
    {ok, State2} = do_put(Key, ?TOMBSTONE, State),
    {reply, ok, State2};

handle_call({get, Key}, _From, State=#state{ top=Top, nursery=Nursery } ) when is_binary(Key) ->
    case hanoi_nursery:lookup(Key, Nursery) of
        {value, ?TOMBSTONE} ->
            {reply, not_found, State};
        {value, Value} when is_binary(Value) ->
            {reply, {ok, Value}, State};
        none ->
            Reply = hanoi_level:lookup(Top, Key),
            {reply, Reply, State}
    end;

handle_call(close, _From, State=#state{top=Top}) ->
    try
        {ok, State2} = flush_nursery(State),
        ok = hanoi_level:close(Top),
        {stop, normal, ok, State2}
    catch
        E:R ->
            error_logger:info_msg("exception from close ~p:~p~n", [E,R]),
            {stop, normal, ok, State}
    end;

handle_call(destroy, _From, State=#state{top=Top, nursery=Nursery }) ->
    ok = hanoi_nursery:destroy(Nursery),
    ok = hanoi_level:destroy(Top),
    {stop, normal, ok, State#state{ top=undefined, nursery=undefined, max_level=?TOP_LEVEL }}.


do_put(Key, Value, State=#state{ nursery=Nursery, top=Top }) ->
    {ok, Nursery2} = hanoi_nursery:add_maybe_flush(Key, Value, Nursery, Top),
    {ok, State#state{ nursery=Nursery2 }}.

do_transact([{put, Key, Value}], State) ->
    do_put(Key, Value, State);
do_transact([{delete, Key}], State) ->
    do_put(Key, ?TOMBSTONE, State);
do_transact([], _State) ->
    ok;
do_transact(TransactionSpec, State=#state{ nursery=Nursery, top=Top }) ->
    {ok, Nursery2} = hanoi_nursery:transact(TransactionSpec, Nursery, Top),
    {ok, State#state{ nursery=Nursery2 }}.

flush_nursery(State=#state{nursery=Nursery, top=Top, dir=Dir, max_level=MaxLevel}) ->
    ok = hanoi_nursery:finish(Nursery, Top),
    {ok, Nursery2} = hanoi_nursery:new(Dir, MaxLevel),
    {ok, State#state{ nursery=Nursery2 }}.

start_app() ->
    application:start(lager),
    case application:start(?MODULE) of
        ok ->
            ok;
        {error, {already_started, ?MODULE}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

get_opt(Key, Opts) ->
    get_opt(Key, Opts, undefined).

get_opt(Key, Opts, Default) ->
    case proplists:get_value(Key, Opts) of
        undefined ->
            case application:get_env(?MODULE, Key) of
                {ok, Value} -> Value;
                undefined -> Default
            end;
        Value ->
            Value
    end.

