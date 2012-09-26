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

-module(hanoidb_nursery).
-author('Kresten Krab Thorup <krab@trifork.com>').

-export([new/3, recover/4, finish/2, lookup/2, add/4, add/5]).
-export([do_level_fold/3, set_max_level/2, transact/3, destroy/1]).

-include("include/hanoidb.hrl").
-include("hanoidb.hrl").
-include_lib("kernel/include/file.hrl").

-spec new(string(), integer(), [_]) -> {ok, #nursery{}} | {error, term()}.

-define(LOGFILENAME(Dir), filename:join(Dir, "nursery.log")).

%% do incremental merge every this many inserts
%% this value *must* be less than or equal to
%% 2^TOP_LEVEL == ?BTREE_SIZE(?TOP_LEVEL)
-define(INC_MERGE_STEP, ?BTREE_SIZE(?TOP_LEVEL)/2).

new(Directory, MaxLevel, Config) ->
    hanoidb_util:ensure_expiry(Config),

    {ok, File} = file:open(?LOGFILENAME(Directory),
                            [raw, exclusive, write, delayed_write, append]),
    {ok, #nursery{ log_file=File, dir=Directory, cache= gb_trees:empty(),
                   max_level=MaxLevel, config=Config }}.


recover(Directory, TopLevel, MaxLevel, Config) ->
    hanoidb_util:ensure_expiry(Config),

    case file:read_file_info(?LOGFILENAME(Directory)) of
        {ok, _} ->
            ok = do_recover(Directory, TopLevel, MaxLevel, Config),
            new(Directory, MaxLevel, Config);
        {error, enoent} ->
            new(Directory, MaxLevel, Config)
    end.

do_recover(Directory, TopLevel, MaxLevel, Config) ->
    %% repair the log file; storing it in nursery2
    LogFileName = ?LOGFILENAME(Directory),
    {ok, Nursery} = read_nursery_from_log(Directory, MaxLevel, Config),
    ok = finish(Nursery, TopLevel),
    %% assert log file is gone
    {error, enoent} = file:read_file_info(LogFileName),
    ok.

fill_cache({Key, Value}, Cache)
  when is_binary(Value); Value =:= ?TOMBSTONE ->
    gb_trees:enter(Key, Value, Cache);
fill_cache({Key, {Value, _TStamp}=Entry}, Cache)
  when is_binary(Value); Value =:= ?TOMBSTONE ->
    gb_trees:enter(Key, Entry, Cache);
fill_cache([], Cache) ->
    Cache;
fill_cache(Transactions, Cache)
  when is_list(Transactions) ->
    lists:foldl(fun fill_cache/2, Cache, Transactions).

read_nursery_from_log(Directory, MaxLevel, Config) ->
    {ok, LogBinary} = file:read_file(?LOGFILENAME(Directory)),
    Cache =
        case hanoidb_util:decode_crc_data(LogBinary, [], []) of
            {ok, KVs} ->
                fill_cache(KVs, gb_trees:empty());
            {partial, KVs, _ErrorData} ->
                error_logger:info_msg("ignoring undecypherable bytes in ~p~n", [?LOGFILENAME(Directory)]),
                fill_cache(KVs, gb_trees:empty())
        end,
    {ok, #nursery{ dir=Directory, cache=Cache, count=gb_trees:size(Cache), max_level=MaxLevel, config=Config }}.

%% @doc Add a Key/Value to the nursery
%% @end
-spec do_add(#nursery{}, binary(), binary()|?TOMBSTONE, non_neg_integer() | infinity, pid()) -> {ok, #nursery{}} | {full, #nursery{}}.
do_add(Nursery, Key, Value, infinity, Top) ->
    do_add(Nursery, Key, Value, 0, Top);
do_add(Nursery=#nursery{log_file=File, cache=Cache, total_size=TotalSize, count=Count, config=Config}, Key, Value, KeyExpiryTime, Top) ->
    DatabaseExpiryTime = hanoidb:get_opt(expiry_secs, Config),

    {Data, Cache2} =
        if (KeyExpiryTime + DatabaseExpiryTime) == 0 ->
                %% Both the database expiry and this key's expiry are unset or set to 0
                %% (aka infinity) so never automatically expire the value.
                { hanoidb_util:crc_encapsulate_kv_entry(Key, Value),
                  gb_trees:enter(Key, Value, Cache) };
           true ->
                Expiry =
                    if DatabaseExpiryTime == 0 ->
                            %% It was the database's setting that was 0 so expire this
                            %% value after KeyExpiryTime seconds elapse.
                            hanoidb_util:expiry_time(KeyExpiryTime);
                       true ->
                            if KeyExpiryTime == 0 ->
                                    hanoidb_util:expiry_time(DatabaseExpiryTime);
                               true ->
                                    hanoidb_util:expiry_time(min(KeyExpiryTime, DatabaseExpiryTime))
                            end
                    end,
                { hanoidb_util:crc_encapsulate_kv_entry(Key, {Value, Expiry}),
                  gb_trees:enter(Key,  {Value, Expiry}, Cache) }
        end,

    ok = file:write(File, Data),
    Nursery1 = do_sync(File, Nursery),
    {ok, Nursery2} = do_inc_merge(Nursery1#nursery{ cache=Cache2,
                                                    total_size=TotalSize + erlang:iolist_size(Data),
                                                    count=Count + 1 }, 1, Top),
    case has_room(Nursery2, 1) of
        true ->
            {ok, Nursery2};
        false ->
            {full, Nursery2}
    end.

do_sync(File, Nursery) ->
    LastSync =
        case application:get_env(hanoidb, sync_strategy) of
            {ok, sync} ->
                file:datasync(File),
                now();
            {ok, {seconds, N}} ->
                MicrosSinceLastSync = timer:now_diff(now(), Nursery#nursery.last_sync),
                if (MicrosSinceLastSync / 1000000) >= N ->
                        file:datasync(File),
                        now();
                   true ->
                        Nursery#nursery.last_sync
                end;
            _ ->
                Nursery#nursery.last_sync
        end,
    Nursery#nursery{last_sync = LastSync}.


lookup(Key, #nursery{cache=Cache}) ->
    case gb_trees:lookup(Key, Cache) of
        {value, {Value, TStamp}} ->
            case hanoidb_util:has_expired(TStamp) of
                true ->
                    {value, ?TOMBSTONE};
                false ->
                    {value, Value}
            end;
        Reply ->
            Reply
    end.

%% @doc
%% Finish this nursery (encode it to a btree, and delete the nursery file)
%% @end
-spec finish(Nursery::#nursery{}, TopLevel::pid()) -> ok.
finish(#nursery{ dir=Dir, cache=Cache, log_file=LogFile, merge_done=DoneMerge,
                 count=Count, config=Config }, TopLevel) ->

    hanoidb_util:ensure_expiry(Config),

    %% First, close the log file (if it is open)
    case LogFile of
        undefined -> ok;
        _ -> ok = file:close(LogFile)
    end,

    case Count of
        N when N > 0 ->
            %% next, flush cache to a new BTree
            BTreeFileName = filename:join(Dir, "nursery.data"),
            {ok, BT} = hanoidb_writer:open(BTreeFileName, [{size, ?BTREE_SIZE(?TOP_LEVEL)},
                                                           {compress, none} | Config]),
            try
                ok = gb_trees_ext:fold(fun(Key, Value, Acc) ->
                                               ok = hanoidb_writer:add(BT, Key, Value),
                                               Acc
                                       end, ok, Cache)
            after
                ok = hanoidb_writer:close(BT)
            end,

            %% Inject the B-Tree (blocking RPC)
            ok = hanoidb_level:inject(TopLevel, BTreeFileName),

            %% Issue some work if this is a top-level inject (blocks until previous such
            %% incremental merge is finished).
            if DoneMerge >= ?BTREE_SIZE(?TOP_LEVEL) ->
                    ok;
               true ->
                    hanoidb_level:begin_incremental_merge(TopLevel, ?BTREE_SIZE(?TOP_LEVEL) - DoneMerge)
            end;
%            {ok, _Nursery2} = do_inc_merge(Nursery, Count, TopLevel);

        _ ->
            ok
    end,

    %% then, delete the log file
    LogFileName = filename:join(Dir, "nursery.log"),
    file:delete(LogFileName),
    ok.

destroy(#nursery{ dir=Dir, log_file=LogFile }) ->
    %% first, close the log file
    if LogFile /= undefined ->
            ok = file:close(LogFile);
       true ->
            ok
    end,
    %% then delete it
    LogFileName = filename:join(Dir, "nursery.log"),
    file:delete(LogFileName),
    ok.

-spec add(key(), value(), #nursery{}, pid()) -> {ok, #nursery{}}.
add(Key, Value, Nursery, Top) ->
    add(Key, Value, infinity, Nursery, Top).

-spec add(key(), value(), expiry(), #nursery{}, pid()) -> {ok, #nursery{}}.
add(Key, Value, Expiry, Nursery, Top) ->
    case do_add(Nursery, Key, Value, Expiry, Top) of
        {ok, Nursery0} ->
            {ok, Nursery0};
        {full, Nursery0} ->
            flush(Nursery0, Top)
    end.

-spec flush(#nursery{}, pid()) -> {ok, #nursery{}}.
flush(Nursery=#nursery{ dir=Dir, max_level=MaxLevel, config=Config }, Top) ->
    ok = finish(Nursery, Top),
    {error, enoent} = file:read_file_info(filename:join(Dir, "nursery.log")),
    hanoidb_nursery:new(Dir, MaxLevel, Config).

has_room(#nursery{ count=Count }, N) ->
    (Count + N + 1) < ?BTREE_SIZE(?TOP_LEVEL).

ensure_space(Nursery, NeededRoom, Top) ->
    case has_room(Nursery, NeededRoom) of
        true ->
            Nursery;
        false ->
            {ok, Nursery1} = flush(Nursery, Top),
            Nursery1
    end.

transact(Spec, Nursery, Top) ->
    transact1(Spec, ensure_space(Nursery, length(Spec), Top), Top).

transact1(Spec, Nursery1=#nursery{ log_file=File, cache=Cache0, total_size=TotalSize, config=Config }, Top) ->
    Expiry =
        case hanoidb:get_opt(expiry_secs, Config) of
            0 ->
                infinity;
            DatabaseExpiryTime ->
                hanoidb_util:expiry_time(DatabaseExpiryTime)
        end,

    Data = hanoidb_util:crc_encapsulate_transaction(Spec, Expiry),
    ok = file:write(File, Data),

    Nursery2 = do_sync(File, Nursery1),

    Cache2 = lists:foldl(fun({put, Key, Value}, Cache) ->
                                 case Expiry of
                                     infinity ->
                                         gb_trees:enter(Key, Value, Cache);
                                     _ ->
                                         gb_trees:enter(Key, {Value, Expiry}, Cache)
                                 end;
                            ({delete, Key}, Cache) ->
                                 case Expiry of
                                     infinity ->
                                         gb_trees:enter(Key, ?TOMBSTONE, Cache);
                                     _ ->
                                         gb_trees:enter(Key, {?TOMBSTONE, Expiry}, Cache)
                                 end
                         end,
                         Cache0,
                         Spec),

    Count = gb_trees:size(Cache2),

    do_inc_merge(Nursery2#nursery{ cache=Cache2, total_size=TotalSize+erlang:iolist_size(Data), count=Count }, length(Spec), Top).

do_inc_merge(Nursery=#nursery{ step=Step, merge_done=Done }, N, TopLevel) ->
    if Step+N >= ?INC_MERGE_STEP ->
            hanoidb_level:begin_incremental_merge(TopLevel, Step + N),
            {ok, Nursery#nursery{ step=0, merge_done=Done + Step + N }};
       true ->
            {ok, Nursery#nursery{ step=Step + N }}
    end.

do_level_fold(#nursery{cache=Cache}, FoldWorkerPID, KeyRange) ->
    Ref = erlang:make_ref(),
    FoldWorkerPID ! {prefix, [Ref]},
    case gb_trees_ext:fold(
           fun(_, _, {LastKey, limit}) ->
                   {LastKey, limit};
              (Key, Value, {LastKey, Count}) ->
                   case ?KEY_IN_RANGE(Key, KeyRange) andalso (not is_expired(Value)) of
                       true ->
                           BinOrTombstone = get_value(Value),
                           FoldWorkerPID ! {level_result, Ref, Key, BinOrTombstone},
                           case BinOrTombstone of
                               ?TOMBSTONE ->
                                   {Key, Count};
                               _ ->
                                   {Key, decrement(Count)}
                           end;
                       false ->
                           {LastKey, Count}
                   end
           end,
           {undefined, KeyRange#key_range.limit},
           Cache)
    of
        {LastKey, limit} when LastKey =/= undefined ->
            FoldWorkerPID ! {level_limit, Ref, LastKey};
        _ ->
            FoldWorkerPID ! {level_done, Ref}
    end,
    ok.

set_max_level(Nursery = #nursery{}, MaxLevel) ->
    Nursery#nursery{ max_level = MaxLevel }.

decrement(undefined) ->
    undefined;
decrement(1) ->
    limit;
decrement(Number) ->
    Number-1.

%%%

% TODO this is duplicate code also found in hanoidb_reader
is_expired(?TOMBSTONE) ->
    false;
is_expired({_Value, TStamp}) ->
    hanoidb_util:has_expired(TStamp);
is_expired(Bin) when is_binary(Bin) ->
    false.

get_value({Value, TStamp}) when is_integer(TStamp); TStamp =:= infinity ->
    Value;
get_value(Value) when Value =:= ?TOMBSTONE; is_binary(Value) ->
    Value.

