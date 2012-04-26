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
-export([do_level_fold/3, set_max_level/2, ensure_space/3]).

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

read_nursery_from_log(Directory, MaxLevel) ->
    {ok, LogFile} = file:open( ?LOGFILENAME(Directory), [raw, read, read_ahead, binary] ),
    {ok, Cache} = load_good_chunks(LogFile, gb_trees:empty()),
    ok = file:close(LogFile),
    {ok, #nursery{ dir=Directory, cache=Cache, count=gb_trees:size(Cache), max_level=MaxLevel }}.

%% Just read the log file into a cache (gb_tree).
%% If any errors happen here, then we simply ignore them and return
%% the values we got so far.
load_good_chunks(File, Cache) ->
    case file:read(File, 8) of
        {ok, <<Length:32, BNotLength:32>>} when BNotLength =:= bnot Length->
            case file:read(File, Length) of
                {ok, EncData} when byte_size(EncData) == Length ->
                    try
                        <<KeySize:32/unsigned, Key:KeySize/binary, ValBin/binary>> = EncData,
                        case ValBin of
                            <<>> -> Value=?TOMBSTONE;
                            <<ValueSize:32/unsigned, Value/binary>> when ValueSize==byte_size(Value) -> ok
                        end,

                        %% TODO: is this tail recursive?  I don't think so
                        load_good_chunks(File, gb_trees:enter(Key, Value, Cache))
                    catch
                        _:_ -> {ok, Cache}
                    end;
                eof ->
                    {ok, Cache};
                {error, _} ->
                    {ok, Cache}
            end;
        _ ->
            {ok, Cache}
    end.


% @doc
% Add a Key/Value to the nursery
% @end
-spec add(#nursery{}, binary(), binary()|?TOMBSTONE) -> {ok, #nursery{}}.
add(Nursery=#nursery{ log_file=File, cache=Cache, total_size=TotalSize, count=Count }, Key, Value) ->

    Size = 4 + byte_size(Key)
        + if Value=:=?TOMBSTONE -> 0;
             true -> 4 + byte_size(Value) end,

    file:write(File, [<<Size:32/unsigned>>, <<(bnot Size):32/unsigned>>,
                      <<(byte_size(Key)):32>>, Key]
                     ++ if Value /= ?TOMBSTONE -> [<<(byte_size(Value)):32>>, Value];
                           true -> [] end),

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

    Cache2 = gb_trees:enter(Key, Value, Cache),
    Nursery2 = Nursery#nursery{ cache=Cache2, total_size=TotalSize+Size+16, count=Count+1, last_sync=LastSync },
    if
       Count+1 >= ?BTREE_SIZE(?TOP_LEVEL) ->
            {full, Nursery2};
       true ->
            {ok, Nursery2}
    end.

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
            hanoi_level:incremental_merge(TopLevel,
                                          (MaxLevel-?TOP_LEVEL+1)*?BTREE_SIZE(?TOP_LEVEL)),

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
    ok = hanoi_nursery:finish(Nursery, Top),
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


do_level_fold(#nursery{ cache=Cache }, FoldWorkerPID, KeyRange) ->
    Ref = erlang:make_ref(),
    FoldWorkerPID ! {prefix, [Ref]},
    lists:foreach(fun({Key,Value}) ->
                          case ?KEY_IN_RANGE(Key,KeyRange) of
                              true ->
                                  FoldWorkerPID ! {level_result, Ref, Key, Value};
                              false ->
                                  ok
                          end
                  end,
                  gb_trees:to_list(Cache)),
    FoldWorkerPID ! {level_done, Ref},
    ok.

set_max_level(Nursery = #nursery{}, MaxLevel) ->
    Nursery#nursery{ max_level = MaxLevel }.
