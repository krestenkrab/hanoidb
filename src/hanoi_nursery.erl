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

-module(hanoi_nursery).
-author('Kresten Krab Thorup <krab@trifork.com>').

-export([new/2, recover/3, add/3, finish/2, lookup/2, add_maybe_flush/4]).
-export([do_level_fold/3, set_max_level/2, transact/3]).

-include("include/hanoi.hrl").
-include("hanoi.hrl").
-include_lib("kernel/include/file.hrl").

-record(nursery, { log_file, dir, cache, total_size=0, count=0,
                   last_sync=now(), max_level }).

-spec new(string(), integer()) -> {ok, #nursery{}} | {error, term()}.

-define(LOGFILENAME(Dir), filename:join(Dir, "nursery.log")).

new(Directory, MaxLevel) ->
    {ok, File} = file:open( ?LOGFILENAME(Directory),
                            [raw, exclusive, write, delayed_write, append]),
    {ok, #nursery{ log_file=File, dir=Directory, cache= gb_trees:empty(),
                   max_level=MaxLevel}}.


recover(Directory, TopLevel, MaxLevel) ->
    case file:read_file_info( ?LOGFILENAME(Directory) ) of
        {ok, _} ->
            ok = do_recover(Directory, TopLevel, MaxLevel),
            new(Directory, MaxLevel);
        {error, enoent} ->
            new(Directory, MaxLevel)
    end.

do_recover(Directory, TopLevel, MaxLevel) ->
    %% repair the log file; storing it in nursery2
    LogFileName = ?LOGFILENAME(Directory),
    {ok, Nursery} = read_nursery_from_log(Directory, MaxLevel),

    ok = finish(Nursery, TopLevel),

    %% assert log file is gone
    {error, enoent} = file:read_file_info(LogFileName),

    ok.

fill_cache({Key,Value}, Cache)
  when is_binary(Value); Value =:= ?TOMBSTONE ->
    gb_trees:enter(Key, Value, Cache);
fill_cache(Transaction, Cache) when is_list(Transaction) ->
    lists:foldl(fun fill_cache/2, Cache, Transaction).

read_nursery_from_log(Directory, MaxLevel) ->
    {ok, LogBinary} = file:read_file( ?LOGFILENAME(Directory) ),
    KVs = hanoi_util:decode_crc_data( LogBinary, [] ),
    Cache = fill_cache(KVs, gb_trees:empty()),
    {ok, #nursery{ dir=Directory, cache=Cache, count=gb_trees:size(Cache), max_level=MaxLevel }}.


% @doc
% Add a Key/Value to the nursery
% @end
-spec add(#nursery{}, binary(), binary()|?TOMBSTONE) -> {ok, #nursery{}}.
add(Nursery=#nursery{ log_file=File, cache=Cache, total_size=TotalSize, count=Count }, Key, Value) ->

    Data = hanoi_util:crc_encapsulate_kv_entry( Key, Value ),
    ok = file:write(File, Data),

    Nursery1 = do_sync(File, Nursery),

    Cache2 = gb_trees:enter(Key, Value, Cache),
    Nursery2 = Nursery1#nursery{ cache=Cache2, total_size=TotalSize+erlang:iolist_size(Data), count=Count+1 },
    if
       Count+1 >= ?BTREE_SIZE(?TOP_LEVEL) ->
            {full, Nursery2};
       true ->
            {ok, Nursery2}
    end.

do_sync(File, Nursery) ->
    case application:get_env(hanoi, sync_strategy) of
        {ok, sync} ->
            file:datasync(File),
            LastSync = now();
        {ok, {seconds, N}} ->
            MicrosSinceLastSync = timer:now_diff(now(), Nursery#nursery.last_sync),
            if (MicrosSinceLastSync / 1000000) >= N ->
                    file:datasync(File),
                    LastSync = now();
               true ->
                    LastSync = Nursery#nursery.last_sync
            end;
        _ ->
            LastSync = Nursery#nursery.last_sync
    end,

    Nursery#nursery{ last_sync = LastSync }.


lookup(Key, #nursery{ cache=Cache }) ->
    gb_trees:lookup(Key, Cache).

% @doc
% Finish this nursery (encode it to a btree, and delete the nursery file)
% @end
-spec finish(Nursery::#nursery{}, TopLevel::pid()) -> ok.
finish(#nursery{ dir=Dir, cache=Cache, log_file=LogFile,
                 total_size=_TotalSize, count=Count,
                 max_level=MaxLevel
               }, TopLevel) ->

    %% first, close the log file (if it is open)
    if LogFile /= undefined ->
            ok = file:close(LogFile);
       true ->
            ok
    end,

    case Count of
        N when N>0 ->
            %% next, flush cache to a new BTree
            BTreeFileName = filename:join(Dir, "nursery.data"),
            {ok, BT} = hanoi_writer:open(BTreeFileName, [{size,?BTREE_SIZE(?TOP_LEVEL)},
                                                         {compress, none}]),
            try
                lists:foreach( fun({Key,Value}) ->
                                       ok = hanoi_writer:add(BT, Key, Value)
                               end,
                               gb_trees:to_list(Cache))
            after
                ok = hanoi_writer:close(BT)
            end,

%            {ok, FileInfo} = file:read_file_info(BTreeFileName),
%            error_logger:info_msg("dumping log (count=~p, size=~p, outsize=~p)~n",
%                                  [ gb_trees:size(Cache), TotalSize, FileInfo#file_info.size ]),


            %% inject the B-Tree (blocking RPC)
            ok = hanoi_level:inject(TopLevel, BTreeFileName),

            %% issue some work if this is a top-level inject (blocks until previous such
            %% incremental merge is finished).
            hanoi_level:begin_incremental_merge(TopLevel),

            ok;

        _ ->
            ok
    end,

    %% then, delete the log file
    LogFileName = filename:join(Dir, "nursery.log"),
    file:delete(LogFileName),
    ok.

add_maybe_flush(Key, Value, Nursery, Top) ->
    case add(Nursery, Key, Value) of
        {ok, _} = OK ->
            OK;
        {full, Nursery2} ->
            flush(Nursery2, Top)
    end.

flush(Nursery=#nursery{ dir=Dir, max_level=MaxLevel }, Top) ->
    ok = finish(Nursery, Top),
    {error, enoent} = file:read_file_info( filename:join(Dir, "nursery.log")),
    hanoi_nursery:new(Dir, MaxLevel).

has_room(#nursery{ count=Count }, N) ->
    (Count+N) < ?BTREE_SIZE(?TOP_LEVEL).

ensure_space(Nursery, NeededRoom, Top) ->
    case has_room(Nursery, NeededRoom) of
        true ->
            Nursery;
        false ->
            flush(Nursery, Top)
    end.

transact(Spec, Nursery=#nursery{ log_file=File, cache=Cache0, total_size=TotalSize }, Top) ->
    Nursery1 = ensure_space(Nursery, length(Spec), Top),

    Data = hanoi_util:crc_encapsulate_transaction( Spec ),
    ok = file:write(File, Data),

    Nursery2 = do_sync(File, Nursery1),

    Cache2 = lists:foldl(fun({put, Key, Value}, Cache) ->
                                 gb_trees:enter(Key, Value, Cache);
                            ({delete, Key}, Cache) ->
                                 gb_trees:enter(Key, ?TOMBSTONE, Cache)
                         end,
                         Cache0,
                         Spec),

    Count = gb_trees:size(Cache2),

    {ok, Nursery2#nursery{ cache=Cache2, total_size=TotalSize+byte_size(Data), count=Count }}.


do_level_fold(#nursery{ cache=Cache }, FoldWorkerPID, KeyRange) ->
    Ref = erlang:make_ref(),
    FoldWorkerPID ! {prefix, [Ref]},
    case lists:foldl(fun(_,{LastKey,limit}) ->
                        {LastKey,limit};
                  ({Key,Value}, {LastKey,Count}) ->
                       case ?KEY_IN_RANGE(Key,KeyRange) of
                           true ->
                               FoldWorkerPID ! {level_result, Ref, Key, Value},
                               case Value of
                                   ?TOMBSTONE ->
                                       {Key, Count};
                                   _ ->
                                       {Key, decrement(Count)}
                               end;
                           false ->
                               {LastKey, Count}
                       end
               end,
               {undefined, KeyRange#btree_range.limit},
               gb_trees:to_list(Cache))
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
