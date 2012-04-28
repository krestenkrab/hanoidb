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

-module(hanoi_writer).
-author('Kresten Krab Thorup <krab@trifork.com>').

-include("hanoi.hrl").

%%
%% Streaming btree writer. Accepts only monotonically increasing keys for put.
%%

-define(NODE_SIZE, 8*1024).

-behavior(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, serialize/1, deserialize/1]).

-export([open/1, open/2, add/3,close/1]).

-record(node, { level, members=[], size=0 }).

-record(state, { index_file,
                 index_file_pos,

                 last_node_pos :: pos_integer(),
                 last_node_size :: pos_integer(),

                 nodes = [] :: [ #node{} ],

                 name :: string(),

                 bloom,
                 block_size = ?NODE_SIZE,
                 compress   = none :: none | snappy | gzip
               }).


%%% PUBLIC API

open(Name,Options) ->
    gen_server:start_link(?MODULE, [Name,Options], []).

open(Name) ->
    gen_server:start_link(?MODULE, [Name,[]], []).


add(Ref,Key,Data) ->
    gen_server:cast(Ref, {add, Key, Data}).

close(Ref) ->
    gen_server:call(Ref, close, infinity).

%%%


init([Name,Options]) ->

    Size = proplists:get_value(size, Options, 2048),

%    io:format("got name: ~p~n", [Name]),
    BlockSize = hanoi:get_opt(block_size, Options, ?NODE_SIZE),
    case file:open( hanoi_util:index_file_name(Name),
                               [raw, exclusive, write, {delayed_write, BlockSize * 4, 2000}]) of
        {ok, IdxFile} ->
            {ok, BloomFilter} = ebloom:new(erlang:min(Size,16#ffffffff), 0.01, 123),
            {ok, #state{ name=Name,
                         index_file_pos=0, index_file=IdxFile,
                         bloom = BloomFilter,
                         block_size = BlockSize,
                         compress = hanoi:get_opt(compress, Options, none)
                       }};
        {error, _}=Error ->
            error_logger:error_msg("hanoi_writer cannot open ~p: ~p~n", [Name, Error]),
            {stop, Error}
    end.


handle_cast({add, Key, Data}, State) when is_binary(Key), (is_binary(Data) orelse Data == ?TOMBSTONE)->
    {ok, State2} = add_record(0, Key, Data, State),
    {noreply, State2}.

handle_call(close, _From, State) ->
    {ok, State2} = flush_nodes(State),
    {stop,normal,ok,State2}.

handle_info(Info,State) ->
    error_logger:error_msg("Unknown info ~p~n", [Info]),
    {stop,bad_msg,State}.


terminate(normal,_State) ->
    ok;

%% premature delete -> cleanup
terminate(_Reason,State) ->
    file:close( State#state.index_file ),
    file:delete( hanoi_util:index_file_name(State#state.name) ).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%%% INTERNAL FUNCTIONS

serialize(#state{ bloom=Bloom }=State) ->
    erlang:term_to_binary( { State, ebloom:serialize(Bloom) } ).

deserialize(Binary) ->
    { State, BinBloom } = erlang:binary_to_term( Binary ),
    {ok, Bloom } = ebloom:deserialize(BinBloom),
    State#state{ bloom = Bloom }.


% @doc flush pending nodes and write trailer

flush_nodes(#state{ nodes=[], last_node_pos=LastNodePos, last_node_size=_LastNodeSize, bloom=Ref }=State) ->

    Bloom = zlib:zip(ebloom:serialize(Ref)),
    BloomSize = byte_size(Bloom),

    Trailer = << 0:32, Bloom/binary, BloomSize:32/unsigned,  LastNodePos:64/unsigned >>,
    IdxFile = State#state.index_file,

    ok = file:write(IdxFile, Trailer),

    ok = file:datasync(IdxFile),

    ok = file:close(IdxFile),

    {ok, State#state{ index_file=undefined }};

%% stack consists of one node with one {pos,len} member.  Just ignore this node.
flush_nodes(State=#state{ nodes=[#node{level=N, members=[{_,{Pos,_Len}}]}], last_node_pos=Pos }) when N>0 ->
    flush_nodes(State#state{ nodes=[] });

flush_nodes(State) ->
    {ok, State2} = close_node(State),
    flush_nodes(State2).

add_record(Level, Key, Value,
           #state{ nodes=[ #node{level=Level, members=List, size=NodeSize}=CurrNode |RestNodes] }=State) ->
    %% The top-of-stack node is at the level we wish to insert at.

    %% Assert that keys are increasing:
    case List of
        [] -> ok;
        [{PrevKey,_}|_] ->
            if
                (Key >= PrevKey) -> ok;
                true ->
                    error_logger:error_msg("keys not ascending ~p < ~p~n", [PrevKey, Key]),
                    exit({badarg, Key})
            end
    end,

    NewSize = NodeSize + hanoi_util:estimate_node_size_increment(List, Key, Value),

    ok = ebloom:insert( State#state.bloom, Key ),

    NodeMembers = [{Key,Value} | List],
    State2 = State#state{ nodes=[CurrNode#node{ members=NodeMembers, size=NewSize} | RestNodes] },
    if
       NewSize >= State#state.block_size ->
            close_node(State2);
        true ->
            {ok, State2}
    end;

add_record(Level, Key, Value, State=#state{ nodes=[] }) ->
    add_record(Level, Key, Value, State#state{ nodes=[ #node{ level=Level } ] });

add_record(Level, Key, Value, State=#state{ nodes=[ #node{level=Level2 } |_]=Stack }) when Level < Level2 ->
    add_record(Level, Key, Value, State#state{ nodes=[ #node{ level=(Level2-1) } | Stack] }).



close_node(#state{nodes=[#node{ level=Level, members=NodeMembers }|RestNodes], compress=Compress} = State) ->
    OrderedMembers = lists:reverse(NodeMembers),
    {ok, BlockData} = hanoi_util:encode_index_node(OrderedMembers, Compress),
    NodePos = State#state.index_file_pos,

    BlockSize = erlang:iolist_size(BlockData),
    Data = [ <<(BlockSize+2):32/unsigned, Level:16/unsigned>> | BlockData ],
    DataSize = BlockSize + 6,

    ok = file:write(State#state.index_file, Data),

    {FirstKey, _} = hd(OrderedMembers),
    add_record(Level+1, FirstKey, {NodePos, DataSize},
               State#state{ nodes          = RestNodes,
                            index_file_pos = NodePos + DataSize,
                            last_node_pos  = NodePos,
                            last_node_size = DataSize
                          }).
