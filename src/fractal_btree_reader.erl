-module(fractal_btree_reader).

-include_lib("kernel/include/file.hrl").

-export([open/1,close/1,lookup/2,fold/3]).

-record(node, { level, members=[] }).
-record(index, {file, root, bloom}).

open(Name) ->

    {ok, File} = file:open(Name, [raw,read,read_ahead,binary]),
    {ok, FileInfo} = file:read_file_info(Name),

    %% read root position
    {ok, <<RootPos:64/unsigned>>} = file:pread(File, FileInfo#file_info.size-8, 8),
    {ok, <<BloomSize:32/unsigned>>} = file:pread(File, FileInfo#file_info.size-12, 4),
    {ok, BloomData} = file:pread(File, FileInfo#file_info.size-12-BloomSize ,BloomSize),

    {ok, Bloom} = ebloom:deserialize(BloomData),

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
    case read_node(File) of
        eof ->
            Acc0;
        {ok, Node} ->
            fold0(File,Fun,Node,Acc0)
    end.

close(#index{file=File}) ->
    file:close(File).


lookup(#index{file=File, root=Node, bloom=Bloom}, Key) ->
    case ebloom:contains(Bloom, Key) of
        true ->
            lookup_in_node(File,Node,Key);
        false ->
            notfound
    end.

lookup_in_node(_File,#node{level=0,members=Members},Key) ->
    case lists:keyfind(Key,1,Members) of
        false ->
            notfound;
        {_,Value} ->
            {ok, Value}
    end;

lookup_in_node(File,#node{members=Members},Key) ->
    case find(Key, Members) of
        {ok, Pos} ->
            {ok, Node} = read_node(File, Pos),
            lookup_in_node(File, Node, Key);
        notfound ->
            notfound
    end.


find(K, [{K1,V},{K2,_}|_]) when K >= K1, K < K2 ->
    {ok, V};
find(K, [{K1,V}]) when K >= K1 ->
    {ok, V};
find(K, [_|T]) ->
    find(K,T);
find(_, _) ->
    notfound.


read_node(File,Pos) ->

    {ok, Pos} = file:position(File, Pos),
    Result = read_node(File),
%    error_logger:info_msg("decoded ~p ~p~n", [Pos, Result]),
    Result.

read_node(File) ->
    {ok, <<Len:32>>} = file:read(File, 4),
    case Len of
        0 -> eof;
        _ ->
            {ok, Data} = file:read(File, Len),
            {ok, Node} = fractal_btree_util:decode_index_node(Data),
            {ok, Node}
    end.


