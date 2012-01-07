-module(basho_bench_driver_lsm_btree).

-record(state, { tree,
                 filename,
                 flags,
                 sync_interval,
                 last_sync }).

-export([new/1,
         run/4]).

-include_lib("basho_bench/include/basho_bench.hrl").


%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    %% Make sure bitcask is available
    case code:which(lsm_btree) of
        non_existing ->
            ?FAIL_MSG("~s requires lsm_btree to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    %% Get the target directory
    Dir = basho_bench_config:get(lsm_btree_dir, "."),
    Filename = filename:join(Dir, "test.frac"),

    %% Look for sync interval config
    case basho_bench_config:get(lsm_btree_sync_interval, infinity) of
        Value when is_integer(Value) ->
            SyncInterval = Value;
        infinity ->
            SyncInterval = infinity
    end,

    %% Get any bitcask flags
    case lsm_btree:open(Filename) of
        {error, Reason} ->
            ?FAIL_MSG("Failed to open lsm btree in ~s: ~p\n", [Filename, Reason]);
        {ok, FBTree} ->
            {ok, #state { tree = FBTree,
                          filename = Filename,
                          sync_interval = SyncInterval,
                          last_sync = os:timestamp() }}
    end.

run(get, KeyGen, _ValueGen, State) ->
    case lsm_btree:lookup(State#state.tree, KeyGen()) of
        {ok, _Value} ->
            {ok, State};
        notfound ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(put, KeyGen, ValueGen, State) ->
    case lsm_btree:put(State#state.tree, KeyGen(), ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    case lsm_btree:delete(State#state.tree, KeyGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

