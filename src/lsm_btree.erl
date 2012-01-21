-module(lsm_btree).

-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([open/1, close/1, lookup/2, delete/2, put/3, async_range/3, async_fold_range/5, sync_range/3, sync_fold_range/5]).

-include("lsm_btree.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, { top, nursery, dir }).


%% PUBLIC API

open(Dir) ->
    gen_server:start(?MODULE, [Dir], []).

close(Ref) ->
    try
        gen_server:call(Ref, close)
    catch
        exit:{noproc,_} -> ok;
        exit:noproc -> ok;
        %% Handle the case where the monitor triggers
        exit:{normal, _} -> ok
    end.


lookup(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {lookup, Key}).

delete(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {delete, Key}).

put(Ref,Key,Value) when is_binary(Key), is_binary(Value) ->
    gen_server:call(Ref, {put, Key, Value}).

sync_range(Ref,FromKey,ToKey) when FromKey == undefined orelse is_binary(FromKey),
                                    ToKey == undefined orelse is_binary(ToKey) ->
    gen_server:call(Ref, {sync_range, self(), FromKey, ToKey}).

sync_fold_range(Ref,Fun,Acc0,FromKey,ToKey) ->
    {ok, PID} = sync_range(Ref,FromKey,ToKey),
    receive_fold_range(PID,Fun,Acc0).

async_range(Ref,FromKey,ToKey) when FromKey == undefined orelse is_binary(FromKey),
                                    ToKey == undefined orelse is_binary(ToKey) ->
    gen_server:call(Ref, {async_range, self(), FromKey, ToKey}).

async_fold_range(Ref,Fun,Acc0,FromKey,ToKey) ->
    {ok, PID} = async_range(Ref,FromKey,ToKey),
    receive_fold_range(PID,Fun,Acc0).

receive_fold_range(PID,Fun,Acc0) ->
    receive
        {fold_result, PID, K,V} ->
            receive_fold_range(PID, Fun, Fun(K,V,Acc0));
        {fold_done, PID} ->
            Acc0
    end.


init([Dir]) ->

    case file:read_file_info(Dir) of
        {ok, #file_info{ type=directory }} ->
            {ok, TopLevel} = open_levels(Dir),
            {ok, Nursery} = lsm_btree_nursery:recover(Dir, TopLevel);

        {error, E} when E =:= enoent ->
            ok = file:make_dir(Dir),
            {ok, TopLevel} = lsm_btree_level:open(Dir, ?TOP_LEVEL, undefined),
            {ok, Nursery} = lsm_btree_nursery:new(Dir)
    end,

    {ok, #state{ top=TopLevel, dir=Dir, nursery=Nursery }}.



open_levels(Dir) ->
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

    TopLevel =
        lists:foldl( fun(LevelNo, Prev) ->
                             {ok, Level} = lsm_btree_level:open(Dir,LevelNo,Prev),
                             Level
                     end,
                     undefined,
                     lists:seq(MaxLevel, MinLevel, -1)),

    {ok, TopLevel}.

parse_level(FileName) ->
    case re:run(FileName, "^[^\\d]+-(\\d+)\\.data$", [{capture,all_but_first,list}]) of
        {match,[StringVal]} ->
            {ok, list_to_integer(StringVal)};
        _ ->
            nomatch
    end.


handle_info(Info,State) ->
    error_logger:error_msg("Unknown info ~p~n", [Info]),
    {stop,bad_msg,State}.

handle_cast(Info,State) ->
    error_logger:error_msg("Unknown cast ~p~n", [Info]),
    {stop,bad_msg,State}.


%% premature delete -> cleanup
terminate(_Reason,_State) ->
    % error_logger:info_msg("got terminate(~p,~p)~n", [Reason,State]),
    % flush_nursery(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



handle_call({async_range, Sender, FromKey, ToKey}, _From, State=#state{ top=TopLevel, nursery=Nursery }) ->
    {ok, FoldWorkerPID} = lsm_btree_fold_worker:start(Sender),
    lsm_btree_nursery:do_level_fold(Nursery, FoldWorkerPID, FromKey, ToKey),
    Result = lsm_btree_level:async_range(TopLevel, FoldWorkerPID, FromKey, ToKey),
    {reply, Result, State};

handle_call({sync_range, Sender, FromKey, ToKey}, _From, State=#state{ top=TopLevel, nursery=Nursery }) ->
    {ok, FoldWorkerPID} = lsm_btree_fold_worker:start(Sender),
    lsm_btree_nursery:do_level_fold(Nursery, FoldWorkerPID, FromKey, ToKey),
    Result = lsm_btree_level:sync_range(TopLevel, FoldWorkerPID, FromKey, ToKey),
    {reply, Result, State};

handle_call({put, Key, Value}, _From, State) when is_binary(Key), is_binary(Value) ->
    {ok, State2} = do_put(Key, Value, State),
    {reply, ok, State2};

handle_call({delete, Key}, _From, State) when is_binary(Key) ->
    {ok, State2} = do_put(Key, ?TOMBSTONE, State),
    {reply, ok, State2};

handle_call({lookup, Key}, _From, State=#state{ top=Top, nursery=Nursery } ) when is_binary(Key) ->
    case lsm_btree_nursery:lookup(Key, Nursery) of
        {value, ?TOMBSTONE} ->
            {reply, notfound, State};
        {value, Value} when is_binary(Value) ->
            {reply, {ok, Value}, State};
        none ->
            Reply = lsm_btree_level:lookup(Top, Key),
            {reply, Reply, State}
    end;

handle_call(close, _From, State=#state{top=Top}) ->
    try
        {ok, State2} = flush_nursery(State),
        ok = lsm_btree_level:close(Top),
        {stop, normal, ok, State2}
    catch
        E:R ->
            error_logger:info_msg("exception from close ~p:~p~n", [E,R]),
            {stop, normal, ok, State}
    end.

do_put(Key, Value, State=#state{ nursery=Nursery, top=Top }) ->
    {ok, Nursery2} = lsm_btree_nursery:add_maybe_flush(Key, Value, Nursery, Top),
    {ok, State#state{ nursery=Nursery2 }}.

flush_nursery(State=#state{nursery=Nursery, top=Top, dir=Dir}) ->
    ok = lsm_btree_nursery:finish(Nursery, Top),
    {ok, Nursery2} = lsm_btree_nursery:new(Dir),
    {ok, State#state{ nursery=Nursery2 }}.
