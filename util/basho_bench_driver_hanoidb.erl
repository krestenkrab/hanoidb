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

-module(basho_bench_driver_hanoidb).

-record(state, { tree :: hanoidb:hanoidb(),
                 filename :: string(),
                 flags :: list(),
                 sync_interval :: integer() | infinity,
                 last_sync :: erlang:timestamp() }).

-export([new/1,
         run/4]).

-include("hanoidb.hrl").
-include_lib("basho_bench/include/basho_bench.hrl").

-record(key_range, { from_key = <<>>       :: binary(),
                       from_inclusive = true :: boolean(),
                       to_key                :: binary() | undefined,
                       to_inclusive = false  :: boolean(),
                       limit :: pos_integer() | undefined }).

%% ====================================================================
%% API
%% ====================================================================

new(_Id) ->
    %% Make sure bitcask is available
    case code:which(hanoidb) of
        non_existing ->
            ?FAIL_MSG("~s requires hanoidb to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    %% Get the target directory
    Dir = basho_bench_config:get(hanoidb_dir, "."),
    Filename = filename:join(Dir, "test.hanoidb"),
    Config = basho_bench_config:get(hanoidb_flags, []),

    %% Look for sync interval config
    SyncInterval =
    case basho_bench_config:get(hanoidb_sync_interval, infinity) of
        Value when is_integer(Value) ->
            Value;
        infinity ->
            infinity
    end,

    %% Get any bitcask flags
    case hanoidb:open(Filename, Config) of
        {error, Reason} ->
            ?FAIL_MSG("Failed to open hanoidb in ~s: ~p\n", [Filename, Reason]);
        {ok, FBTree} ->
            {ok, #state { tree = FBTree,
                          filename = Filename,
                          sync_interval = SyncInterval,
                          last_sync = os:timestamp() }}
    end.

run(get, KeyGen, _ValueGen, State) ->
    case hanoidb:lookup(State#state.tree, KeyGen()) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(put, KeyGen, ValueGen, State) ->
    case hanoidb:put(State#state.tree, KeyGen(), ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    case hanoidb:delete(State#state.tree, KeyGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;

run(fold_100, KeyGen, _ValueGen, State) ->
    [From,To] = lists:usort([KeyGen(), KeyGen()]),
    case hanoidb:fold_range(State#state.tree,
                                   fun(_Key,_Value,Count) ->
                                           Count+1
                                   end,
                                   0,
                                   #key_range{ from_key=From,
                                                 to_key=To,
                                                 limit=100 }) of
        Count when Count >= 0; Count =< 100 ->
            {ok,State};
        Count ->
            {error, {bad_fold_count, Count}}
    end.
