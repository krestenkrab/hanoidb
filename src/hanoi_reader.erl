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

-module(hanoi_reader).
-author('Kresten Krab Thorup <krab@trifork.com>').

-include_lib("kernel/include/file.hrl").
-include("include/hanoi.hrl").
-include("hanoi.hrl").
-include("include/plain_rpc.hrl").

-export([open/1, open/2,close/1,lookup/2,fold/3,range_fold/4]).
-export([first_node/1,next_node/1]).

-record(node, { level, members=[] }).
-record(index, {file, root, bloom}).

-type read_file() :: #index{}.

-spec open(Name::string()) -> read_file().
open(Name) ->
    open(Name, random).

-spec open(Name::string(), sequential|random) -> read_file().

%% this is how to open a btree for sequential scanning (merge, fold)
open(Name, sequential) ->
    {ok, File} = file:open(Name, [raw,read,{read_ahead, 1024 * 512},binary]),
    open2(Name, File);

%% this is how to open a btree for random access
open(Name, random) ->
    {ok, File} = file:open(Name, [read,binary]),
    open2(Name, File).

open2(Name, File) ->
    {ok, FileInfo} = file:read_file_info(Name),

    %% read root position
    {ok, <<RootPos:64/unsigned>>} = file:pread(File, FileInfo#file_info.size-8, 8),
    {ok, <<BloomSize:32/unsigned>>} = file:pread(File, FileInfo#file_info.size-12, 4),
    {ok, BloomData} = file:pread(File, FileInfo#file_info.size-12-BloomSize ,BloomSize),

    {ok, Bloom} = ebloom:deserialize(zlib:unzip(BloomData)),

    %% suck in the root
    {ok, Root} = read_node(File, RootPos),

    {ok, #index{file=File, root=Root, bloom=Bloom}}.


fold(Fun, Acc0, #index{file=File}) ->
    {ok, Node} = read_node(File,0),
    fold0(File,fun({K,V},Acc) -> Fun(K,V,Acc) end,Node,Acc0).

fold0(File,Fun,#node{level=0,members=List},Acc0) ->
    Acc1 = lists:foldl(Fun,Acc0,List),
    fold1(File,Fun,Acc1);
fold0(File,Fun,_InnerNode,Acc0) ->
    fold1(File,Fun,Acc0).

fold1(File,Fun,Acc0) ->
    case next_leaf_node(File) of
        eof ->
            Acc0;
        {ok, Node} ->
            fold0(File,Fun,Node,Acc0)
    end.

range_fold(Fun, Acc0, #index{file=File,root=Root}, Range) ->
    case lookup_node(File,Range#btree_range.from_key,Root,0) of
        {ok, {Pos,_}} ->
            file:position(File, Pos),
            do_range_fold(Fun, Acc0, File, Range, Range#btree_range.limit);
        {ok, Pos} ->
            file:position(File, Pos),
            do_range_fold(Fun, Acc0, File, Range, Range#btree_range.limit);
        none ->
            {done, Acc0}
    end.

fold_until_stop(Fun,Acc,List) ->
    fold_until_stop2(Fun, {continue, Acc}, List).

fold_until_stop2(_Fun,{stop,Result},_) ->
    {stopped, Result};
fold_until_stop2(_Fun,{continue, Acc},[]) ->
    {ok, Acc};
fold_until_stop2(Fun,{continue, Acc},[H|T]) ->
    fold_until_stop2(Fun,Fun(H,Acc),T).

do_range_fold(Fun, Acc0, File, Range, undefined) ->
    case next_leaf_node(File) of
        eof ->
            {done, Acc0};

        {ok, #node{members=Members}} ->
            case fold_until_stop(fun({Key,_}, Acc) when not ?KEY_IN_TO_RANGE(Key,Range) ->
                                         {stop, {done, Acc}};
                                    ({Key,Value}, Acc) when ?KEY_IN_FROM_RANGE(Key, Range) ->
                                         {continue, Fun(Key, Value, Acc)};
                                    (_, Acc) ->
                                         {continue, Acc}
                                 end,
                                 Acc0,
                                 Members) of
                {stopped, Result} -> Result;
                {ok, Acc1} ->
                    do_range_fold(Fun, Acc1, File, Range, undefined)
            end
    end;

do_range_fold(Fun, Acc0, File, Range, N0) ->
    case next_leaf_node(File) of
        eof ->
            {done, Acc0};

        {ok, #node{members=Members}} ->
            case fold_until_stop(fun({Key,_}, {0,Acc}) ->
                                         {stop, {limit, Acc, Key}};
                                    ({Key,_}, {_,Acc}) when not ?KEY_IN_TO_RANGE(Key,Range)->
                                         {stop, {done, Acc}};
                                    ({Key,Value}, {N1,Acc}) when ?KEY_IN_FROM_RANGE(Key,Range) ->
                                         {continue, {N1-1, Fun(Key, Value, Acc)}};
                                    (_, Acc) ->
                                         {continue, Acc}
                                 end,
                                 {N0, Acc0},
                                 Members) of
                {stopped, Result} -> Result;
                {ok, {N2, Acc1}} ->
                    do_range_fold(Fun, Acc1, File, Range, N2)
            end
    end.

lookup_node(_File,_FromKey,#node{level=0},Pos) ->
    {ok, Pos};
lookup_node(File,FromKey,#node{members=Members,level=N},_) ->
    case find_start(FromKey, Members) of
        {ok, ChildPos} when N==1 ->
            {ok, ChildPos};
        {ok, ChildPos} ->
            case read_node(File,ChildPos) of
                {ok, ChildNode} ->
                    lookup_node(File,FromKey,ChildNode,ChildPos);
                eof ->
                    none
            end;
        not_found ->
            none
    end.



first_node(#index{file=File}) ->
    case read_node(File, 0) of
        {ok, #node{level=0, members=Members}} ->
            {node, Members}
    end.

next_node(#index{file=File}=_Index) ->
    case next_leaf_node(File) of
        {ok, #node{level=0, members=Members}} ->
            {node, Members};
%        {ok, #node{level=N}} when N>0 ->
%            next_node(Index);
        eof ->
            end_of_data
    end.

close(#index{file=File}) ->
    file:close(File).


lookup(#index{file=File, root=Node, bloom=Bloom}, Key) ->
    case ebloom:contains(Bloom, Key) of
        true ->
            lookup_in_node(File,Node,Key);
        false ->
            not_found
    end.

lookup_in_node(_File,#node{level=0,members=Members},Key) ->
    case lists:keyfind(Key,1,Members) of
        false ->
            not_found;
        {_,Value} ->
            {ok, Value}
    end;

lookup_in_node(File,#node{members=Members},Key) ->
    case find_1(Key, Members) of
        {ok, {Pos,Size}} ->
            %% do this in separate process, to avoid having to
            %% garbage collect all the inner node junk
            PID = proc_lib:spawn_link(fun() ->
                                              receive
                                                  ?CALL(From,read) ->
                                                      {ok, Node} = read_node(File, {Pos,Size}),
                                                      Result = lookup_in_node2(File, Node, Key),
                                                      plain_rpc:send_reply(From, Result)
                                              end
                                      end),
            plain_rpc:call(PID, read);
        not_found ->
            not_found
    end.


lookup_in_node2(_File,#node{level=0,members=Members},Key) ->
    case lists:keyfind(Key,1,Members) of
        false ->
            not_found;
        {_,Value} ->
            {ok, Value}
    end;

lookup_in_node2(File,#node{members=Members},Key) ->
    case find_1(Key, Members) of
        {ok, {Pos,Size}} ->
            {ok, Node} = read_node(File, {Pos,Size}),
            lookup_in_node2(File, Node, Key);
        not_found ->
            not_found
    end.


find_1(K, [{K1,V},{K2,_}|_]) when K >= K1, K < K2 ->
    {ok, V};
find_1(K, [{K1,V}]) when K >= K1 ->
    {ok, V};
find_1(K, [_|T]) ->
    find_1(K,T);
find_1(_, _) ->
    not_found.


find_start(K, [{_,V},{K2,_}|_]) when K < K2 ->
    {ok, V};
find_start(_, [{_,{_,_}=V}]) ->
    {ok, V};
find_start(K, KVs) ->
    find_1(K, KVs).




read_node(File,{Pos,Size}) ->
%   error_logger:info_msg("read_node ~p ~p ~p~n", [File, Pos, Size]),
    {ok, <<_:32, Level:16/unsigned, Data/binary>>} = file:pread(File, Pos, Size),
    hanoi_util:decode_index_node(Level, Data);

read_node(File,Pos) ->
    {ok, Pos} = file:position(File, Pos),
    Result = read_node(File),
%   error_logger:info_msg("decoded ~p ~p~n", [Pos, Result]),
    Result.

read_node(File) ->
    {ok, <<Len:32, Level:16/unsigned>>} = file:read(File, 6),
    case Len of
        0 -> eof;
        _ ->
            {ok, Data} = file:read(File, Len-2),
            hanoi_util:decode_index_node(Level, Data)
    end.


next_leaf_node(File) ->
    case file:read(File, 6) of
        {ok, <<0:32, _:16>>} ->
            eof;
        {ok, <<Len:32, 0:16>>} ->
            {ok, Data} = file:read(File, Len-2),
            hanoi_util:decode_index_node(0, Data);
        {ok, <<Len:32, _:16>>} ->
            {ok, _} = file:position(File, {cur,Len-2}),
            next_leaf_node(File)
    end.

