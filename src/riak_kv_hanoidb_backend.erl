%% ----------------------------------------------------------------------------
%%
%% hanoidb: LSM-trees (Log-Structured Merge Trees) Indexed Storage
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

-module(riak_kv_hanoidb_backend).
-behavior(hanoidb_temp_riak_kv_backend).
-author('Steve Vinoski <steve@basho.com>').
-author('Greg Burd <greg@basho.com>').

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).


-define(log(Fmt,Args),ok).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([to_index_key/4,from_index_key/1,
         to_object_key/2,from_object_key/1,
         to_key_range/1]).
-endif.

-include("include/hanoidb.hrl").

-define(API_VERSION, 1).
%% TODO: for when this backend supports 2i
-define(CAPABILITIES, [async_fold, indexes]).
%-define(CAPABILITIES, [async_fold]).

-record(state, {tree,
                partition :: integer(),
                config :: config() }).

-type state() :: #state{}.
-type config_option() :: {data_root, string()} | hanoidb:config_option().
-type config() :: [config_option()].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the hanoidb backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    %% Get the data root directory
    case app_helper:get_prop_or_env(data_root, Config, hanoidb) of
        undefined ->
            lager:error("Failed to create hanoidb dir: data_root is not set"),
            {error, data_root_unset};
        DataRoot ->
            AppStart = case application:start(hanoidb) of
                           ok ->
                               ok;
                           {error, {already_started, _}} ->
                               ok;
                           {error, StartReason} ->
                               lager:error("Failed to init the hanoidb backend: ~p", [StartReason]),
                               {error, StartReason}
                       end,
            case AppStart of
                ok ->
                    case get_data_dir(DataRoot, integer_to_list(Partition)) of
                        {ok, DataDir} ->
                            case hanoidb:open(DataDir, Config) of
                                {ok, Tree} ->
                                    {ok, #state{tree=Tree, partition=Partition, config=Config }};
                                {error, OpenReason}=OpenError ->
                                    lager:error("Failed to open hanoidb: ~p\n", [OpenReason]),
                                    OpenError
                            end;
                        {error, Reason} ->
                            lager:error("Failed to start hanoidb backend: ~p\n", [Reason]),
                            {error, Reason}
                    end;
                Error ->
                    Error
            end
    end.

%% @doc Stop the hanoidb backend
-spec stop(state()) -> ok.
stop(#state{tree=Tree}) ->
    ok = hanoidb:close(Tree).

%% @doc Retrieve an object from the hanoidb backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{tree=Tree}=State) ->
    BKey = to_object_key(Bucket, Key),
    case hanoidb:get(Tree, BKey) of
        {ok, Value} ->
            {ok, Value, State};
        not_found  ->
            {error, not_found, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Insert an object into the hanoidb backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, PrimaryKey, IndexSpecs, Val, #state{tree=Tree}=State) ->
    %% Create the KV update...
    StorageKey = to_object_key(Bucket, PrimaryKey),
    Updates1 = [{put, StorageKey, Val}],

    %% Convert IndexSpecs to index updates...
    F = fun({add, Field, Value}) ->
                {put, to_index_key(Bucket, PrimaryKey, Field, Value), <<>>};
           ({remove, Field, Value}) ->
                {delete, to_index_key(Bucket, PrimaryKey, Field, Value)}
        end,
    Updates2 = [F(X) || X <- IndexSpecs],

    ok = hanoidb:transact(Tree, Updates1 ++ Updates2),
    {ok, State}.

%% @doc Delete an object from the hanoidb backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, PrimaryKey, IndexSpecs, #state{tree=Tree}=State) ->

    %% Create the KV delete...
    StorageKey = to_object_key(Bucket, PrimaryKey),
    Updates1 = [{delete, StorageKey}],

    %% Convert IndexSpecs to index deletes...
    F = fun({remove, Field, Value}) ->
                {delete, to_index_key(Bucket, PrimaryKey, Field, Value)}
        end,
    Updates2 = [F(X) || X <- IndexSpecs],

    case hanoidb:transact(Tree, Updates1 ++ Updates2) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{tree=Tree}) ->
    BucketFolder =
        fun() ->
                fold_list_buckets(undefined, Tree, FoldBucketsFun, Acc)
        end,
    case proplists:get_bool(async_fold, Opts) of
        true ->
            {async, BucketFolder};
        false ->
            {ok, BucketFolder()}
    end.


fold_list_buckets(PrevBucket, Tree, FoldBucketsFun, Acc) ->
    ?log("fold_list_buckets prev=~p~n", [PrevBucket]),
    case PrevBucket of
        undefined ->
            RangeStart = to_object_key(<<>>, '_');
        _ ->
            RangeStart = to_object_key(<<PrevBucket/binary, 0>>, '_')
    end,

    Range = #key_range{ from_key=RangeStart, from_inclusive=true,
                          to_key=undefined, to_inclusive=undefined,
                          limit=1 },

    %% grab next bucket, it's a limit=1 range query :-)
    case hanoidb:fold_range(Tree,
                          fun(BucketKey,_Value,none) ->
                                  ?log( "IN_FOLDER ~p~n", [BucketKey]),
                                  case from_object_key(BucketKey) of
                                      {Bucket, _Key} ->
                                          [Bucket];
                                      _ ->
                                          none
                                  end
                          end,
                          none,
                          Range)
    of
        none ->
            ?log( "NO_MORE_BUCKETS~n", []),
            Acc;
        [Bucket] ->
            ?log( "NEXT_BUCKET ~p~n", [Bucket]),
            fold_list_buckets(Bucket, Tree, FoldBucketsFun, FoldBucketsFun(Bucket, Acc))
    end.


%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{tree=Tree}) ->
    %% Figure out how we should limit the fold: by bucket, by
    %% secondary index, or neither (fold across everything.)
    Bucket = lists:keyfind(bucket, 1, Opts),
    Index = lists:keyfind(index, 1, Opts),

    %% Multiple limiters may exist. Take the most specific limiter.
    Limiter =
        if Index /= false  -> Index;
           Bucket /= false -> Bucket;
           true            -> undefined
        end,

    %% Set up the fold...
    FoldFun = fold_keys_fun(FoldKeysFun, Limiter),
    Range   = to_key_range(Limiter),
    case proplists:get_bool(async_fold, Opts) of
        true ->
            {async, fun() -> hanoidb:fold_range(Tree, FoldFun, Acc, Range) end};
        false ->
            {ok, hanoidb:fold_range(Tree, FoldFun, Acc, Range)}
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{tree=Tree}) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    ObjectFolder =
        fun() ->
%                io:format(user, "starting fold_objects in ~p~n", [self()]),
                Result = hanoidb:fold_range(Tree, FoldFun, Acc, to_key_range(Bucket)),
%                io:format(user, "ended fold_objects in ~p => ~P~n", [self(),Result,20]),
                Result
        end,
    case proplists:get_bool(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end.

%% @doc Delete all objects from this hanoidb backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{ tree=Tree, partition=Partition, config=Config }=State) ->
    case hanoidb:destroy(Tree) of
        ok ->
            start(Partition, Config);
        {error, Term} ->
            {error, Term, State}
    end.

%% @doc Returns true if this hanoidb backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(#state{tree=Tree}) ->
    FoldFun = fun(K, _V, Acc) -> [K|Acc] end,
    try
        Range = to_key_range(undefined),
        [] =:= hanoidb:fold_range(Tree, FoldFun, [], Range#key_range{ limit=1 })
    catch
        _:ok ->
            false
    end.

%% @doc Get the status information for this hanoidb backend
-spec status(state()) -> [{atom(), term()}].
status(#state{}) ->
    %% TODO: not yet implemented
    [].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% Create the directory for this partition's LSM-BTree files
get_data_dir(DataRoot, Partition) ->
    PartitionDir = filename:join([DataRoot, Partition]),
    case filelib:ensure_dir(filename:join([filename:absname(DataRoot), Partition, "x"])) of
        ok ->
            {ok, PartitionDir};
        {error, Reason} ->
            lager:error("Failed to create hanoidb dir ~s: ~p", [PartitionDir, Reason]),
            {error, Reason}
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, undefined) ->
    %% Fold across everything...
    fun(K, _V, Acc) ->
            case from_object_key(K) of
                {Bucket, Key} ->
                    FoldKeysFun(Bucket, Key, Acc)
            end
    end;
fold_keys_fun(FoldKeysFun, {bucket, FilterBucket}) ->
    %% Fold across a specific bucket...
    fun(K, _V, Acc) ->
            case from_object_key(K) of
                {Bucket, Key} when Bucket == FilterBucket ->
                    FoldKeysFun(Bucket, Key, Acc)
            end
    end;
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, <<"$bucket">>, _}}) ->
    %% 2I exact match query on special $bucket field...
    fold_keys_fun(FoldKeysFun, {bucket, FilterBucket});
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, FilterField, FilterTerm}}) ->
    %% Rewrite 2I exact match query as a range...
    NewQuery = {range, FilterField, FilterTerm, FilterTerm},
    fold_keys_fun(FoldKeysFun, {index, FilterBucket, NewQuery});
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, <<"$key">>, StartKey, EndKey}}) ->
    %% 2I range query on special $key field...
    fun(StorageKey, Acc) ->
            case from_object_key(StorageKey) of
                {Bucket, Key} when FilterBucket == Bucket,
                                   StartKey =< Key,
                                   EndKey >= Key ->
                    FoldKeysFun(Bucket, Key, Acc)
            end
    end;
fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, FilterField, StartTerm, EndTerm}}) ->
    %% 2I range query...
    fun(StorageKey, Acc) ->
            case from_index_key(StorageKey) of
                {Bucket, Key, Field, Term} when FilterBucket == Bucket,
                                                FilterField == Field,
                                                StartTerm =< Term,
                                                EndTerm >= Term ->
                    FoldKeysFun(Bucket, Key, Acc)
            end
    end;
fold_keys_fun(_FoldKeysFun, Other) ->
    throw({unknown_limiter, Other}).

%% @private
%% Return a function to fold over the objects on this backend
fold_objects_fun(FoldObjectsFun, FilterBucket) ->
    fun(StorageKey, Value, Acc) ->
            ?log( "OFOLD: ~p, filter=~p~n", [sext:decode(StorageKey), FilterBucket]),
            case from_object_key(StorageKey) of
                {Bucket, Key} when FilterBucket == undefined;
                                   Bucket == FilterBucket ->
                    FoldObjectsFun(Bucket, Key, Value, Acc)
            end
    end.


%% This is guaranteed larger than any object key
-define(MAX_OBJECT_KEY, <<16,0,0,0,4>>).

%% This is guaranteed larger than any index key
-define(MAX_INDEX_KEY, <<16,0,0,0,6>>).

to_key_range(undefined) ->
    #key_range{ from_key       = to_object_key(<<>>, <<>>),
                  from_inclusive = true,
                  to_key         = ?MAX_OBJECT_KEY,
                  to_inclusive   = false
                };
to_key_range({bucket, Bucket}) ->
    #key_range{ from_key       = to_object_key(Bucket, <<>>),
                  from_inclusive = true,
                  to_key         = to_object_key(<<Bucket/binary, 0>>, <<>>),
                  to_inclusive   = false };
to_key_range({index, Bucket, {eq, <<"$bucket">>, _Term}}) ->
    to_key_range(Bucket);
to_key_range({index, Bucket, {eq, Field, Term}}) ->
    to_key_range({index, Bucket, {range, Field, Term, Term}});
to_key_range({index, Bucket, {range, <<"$key">>, StartTerm, EndTerm}}) ->
    #key_range{ from_key       = to_object_key(Bucket, StartTerm),
                  from_inclusive = true,
                  to_key         = to_object_key(Bucket, EndTerm),
                  to_inclusive   = true };
to_key_range({index, Bucket, {range, Field, StartTerm, EndTerm}}) ->
    #key_range{ from_key       = to_index_key(Bucket, <<>>, Field, StartTerm),
                  from_inclusive = true,
                  to_key         = to_index_key(Bucket, <<16#ff,16#ff,16#ff,16#ff,
                                                          16#ff,16#ff,16#ff,16#ff,
                                                          16#ff,16#ff,16#ff,16#ff,
                                                          16#ff,16#ff,16#ff,16#ff,
                                                          16#ff,16#ff,16#ff,16#ff,
                                                          16#ff,16#ff,16#ff,16#ff,
                                                          16#ff,16#ff,16#ff,16#ff,
                                                          16#ff,16#ff,16#ff,16#ff >>, Field, EndTerm),
                  to_inclusive   = false };
to_key_range(Other) ->
    erlang:throw({unknown_limiter, Other}).




to_object_key(Bucket, Key) ->
    sext:encode({o, Bucket, Key}).

from_object_key(LKey) ->
    case sext:decode(LKey) of
        {o, Bucket, Key} ->
            {Bucket, Key};
        _ ->
            undefined
    end.

to_index_key(Bucket, Key, Field, Term) ->
    sext:encode({i, Bucket, Field, Term, Key}).

from_index_key(LKey) ->
    case sext:decode(LKey) of
        {i, Bucket, Field, Term, Key} ->
            {Bucket, Key, Field, Term};
        _ ->
            undefined
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-include("src/hanoidb.hrl").

key_range_test() ->
    Range = to_key_range({bucket, <<"a">>}),

    ?assertEqual(true,  ?KEY_IN_RANGE( to_object_key(<<"a">>, <<>>) , Range)),
    ?assertEqual(true,  ?KEY_IN_RANGE( to_object_key(<<"a">>, <<16#ff,16#ff,16#ff,16#ff>>), Range )),
    ?assertEqual(false, ?KEY_IN_RANGE( to_object_key(<<>>, <<>>), Range )),
    ?assertEqual(false, ?KEY_IN_RANGE( to_object_key(<<"a",0>>, <<>>), Range )).

index_range_test() ->
    Range = to_key_range({index, <<"idx">>, {range, <<"f">>, <<6>>, <<7,3>>}}),

    ?assertEqual(false, ?KEY_IN_RANGE( to_index_key(<<"idx">>, <<"key1">>, <<"f">>, <<5>>) , Range)),
    ?assertEqual(true,  ?KEY_IN_RANGE( to_index_key(<<"idx">>, <<"key1">>, <<"f">>, <<6>>) , Range)),
    ?assertEqual(true,  ?KEY_IN_RANGE( to_index_key(<<"idx">>, <<"key1">>, <<"f">>, <<7>>) , Range)),
    ?assertEqual(false, ?KEY_IN_RANGE( to_index_key(<<"idx">>, <<"key1">>, <<"f">>, <<7,4>>) , Range)),
    ?assertEqual(false, ?KEY_IN_RANGE( to_index_key(<<"idx">>, <<"key1">>, <<"f">>, <<9>>) , Range)).


simple_test_() ->
    ?assertCmd("rm -rf test/hanoidb-backend"),
    application:set_env(hanoidb, data_root, "test/hanoidbd-backend"),
    hanoidb_temp_riak_kv_backend:standard_test(?MODULE, []).

custom_config_test_() ->
    ?assertCmd("rm -rf test/hanoidb-backend"),
    application:set_env(hanoidb, data_root, ""),
    hanoidb_temp_riak_kv_backend:standard_test(?MODULE, [{data_root, "test/hanoidb-backend"}]).

-ifdef(PROPER).

eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          {timeout, 60,
           [?_assertEqual(true,
                          backend_eqc:test(?MODULE, false,
                                           [{data_root,
                                             "test/hanoidbdb-backend"},
                                         {async_fold, false}]))]},
          {timeout, 60,
            [?_assertEqual(true,
                          backend_eqc:test(?MODULE, false,
                                           [{data_root,
                                             "test/hanoidbdb-backend"}]))]}
         ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "riak_kv_hanoidbdb_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_hanoidbdb_backend_eqc.log"}),

    ok.

cleanup(_) ->
    ?_assertCmd("rm -rf test/hanoidbdb-backend").

-endif. % EQC


-endif.
