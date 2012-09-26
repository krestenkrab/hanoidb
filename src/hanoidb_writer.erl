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

-module(hanoidb_writer).
-author('Kresten Krab Thorup <krab@trifork.com>').

-include("hanoidb.hrl").

%%
%% Streaming btree writer. Accepts only monotonically increasing keys for put.
%%

-define(NODE_SIZE, 8*1024).

-behavior(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, serialize/1, deserialize/1]).

-export([open/1, open/2, add/3, count/1, close/1]).

-record(node, {level      :: integer(),
               members=[] :: [ {key(), expvalue()} ],
               size=0     :: integer()}).

-record(state, {index_file               :: file:io_device() | undefined,
                index_file_pos           :: integer(),

                last_node_pos            :: pos_integer(),
                last_node_size           :: pos_integer(),

                nodes = []               :: list(#node{}),

                name                     :: string(),

                bloom                    :: term(),
                block_size = ?NODE_SIZE  :: integer(),
                compress  = none         :: none | snappy | gzip, % | lz4,
                opts = []                :: list(any()),

                value_count = 0          :: integer(),
                tombstone_count = 0      :: integer()
               }).


%%% PUBLIC API

open(Name,Options) ->
    hanoidb_util:ensure_expiry(Options),
    gen_server:start_link(?MODULE, [Name, Options], []).

open(Name) ->
    gen_server:start_link(?MODULE, [Name,[{expiry_secs,0}]], []).

add(Ref, Key, Value) ->
    gen_server:cast(Ref, {add, Key, Value}).

%% @doc Return number of KVs added to this writer so far
count(Ref) ->
    gen_server:call(Ref, count, infinity).

%% @doc Close the btree index file
close(Ref) ->
    gen_server:call(Ref, close, infinity).

%%%

init([Name, Options]) ->
    hanoidb_util:ensure_expiry(Options),
    Size = proplists:get_value(size, Options, 2048),

    case do_open(Name, Options, [exclusive]) of
        {ok, IdxFile} ->
            ok = file:write(IdxFile, ?FILE_FORMAT),
            Bloom = hanoidb_bloom:bloom(Size),
            BlockSize = hanoidb:get_opt(block_size, Options, ?NODE_SIZE),
            {ok, #state{ name=Name,
                         index_file_pos=?FIRST_BLOCK_POS, index_file=IdxFile,
                         bloom = Bloom,
                         block_size = BlockSize,
                         compress = hanoidb:get_opt(compress, Options, none),
                         opts = Options
                       }};
        {error, _}=Error ->
            error_logger:error_msg("hanoidb_writer cannot open ~p: ~p~n", [Name, Error]),
            {stop, Error}
    end.


handle_cast({add, Key, {?TOMBSTONE, TStamp}}, State)
  when is_binary(Key) ->
    NewState =
        case hanoidb_util:has_expired(TStamp) of
            true ->
                State;
            false ->
                {ok, State2} = append_node(0, Key, {?TOMBSTONE, TStamp}, State),
                State2
        end,
    {noreply, NewState};
handle_cast({add, Key, ?TOMBSTONE}, State)
  when is_binary(Key) ->
    {ok, NewState} = append_node(0, Key, ?TOMBSTONE, State),
    {noreply, NewState};
handle_cast({add, Key, {Value, TStamp}}, State)
  when is_binary(Key), is_binary(Value) ->
    NewState =
        case hanoidb_util:has_expired(TStamp) of
            true ->
                State;
            false ->
                {ok, State2} = append_node(0, Key, {Value, TStamp}, State),
                State2
        end,
    {noreply, NewState};
handle_cast({add, Key, Value}, State)
  when is_binary(Key), is_binary(Value) ->
    {ok, State2} = append_node(0, Key, Value, State),
    {noreply, State2}.

handle_call(count, _From, State = #state{ value_count=VC, tombstone_count=TC }) ->
    {ok, VC+TC, State};
handle_call(close, _From, State) ->
    {ok, State2} = archive_nodes(State),
    {stop, normal, ok, State2}.

handle_info(Info, State) ->
    error_logger:error_msg("Unknown info ~p~n", [Info]),
    {stop, bad_msg, State}.

terminate(normal,_State) ->
    ok;
terminate(_Reason, State) ->
    %% premature delete -> cleanup
    _ignore = file:close(State#state.index_file),
    file:delete(hanoidb_util:index_file_name(State#state.name)).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% INTERNAL FUNCTIONS
serialize(#state{ bloom=Bloom, index_file=File, index_file_pos=Position }=State) ->
    case file:position(File, {eof, 0}) of
        {ok, Position} ->
            ok;
        {ok, WrongPosition} ->
            exit({bad_position, Position, WrongPosition})
    end,
    ok = file:close(File),
    erlang:term_to_binary( { State#state{ index_file=undefined }, hanoidb_bloom:encode(Bloom) } ).

deserialize(Binary) ->
    {State, Bin} = erlang:binary_to_term(Binary),
    Bloom = hanoidb_bloom:decode(Bin),
    {ok, IdxFile} = do_open(State#state.name, State#state.opts, []),
    State#state{ bloom=Bloom, index_file=IdxFile }.


do_open(Name, Options, OpenOpts) ->
    WriteBufferSize = hanoidb:get_opt(write_buffer_size, Options, 512 * 1024),
    file:open(hanoidb_util:index_file_name(Name),
              [raw, append, {delayed_write, WriteBufferSize, 2000} | OpenOpts]).


%% @doc flush pending nodes and write trailer
archive_nodes(#state{ nodes=[], last_node_pos=LastNodePos, last_node_size=_LastNodeSize, bloom=Bloom, index_file=IdxFile }=State) ->

    BloomBin = hanoidb_bloom:encode(Bloom),
    BloomSize = byte_size(BloomBin),
    RootPos =
        case LastNodePos of
            undefined ->
                %% store contains no entries
                ok = file:write(IdxFile, <<0:32/unsigned, 0:16/unsigned>>),
                ?FIRST_BLOCK_POS;
            _ ->
                LastNodePos
        end,
    Trailer = << 0:32/unsigned, BloomBin/binary, BloomSize:32/unsigned,  RootPos:64/unsigned >>,

    ok = file:write(IdxFile, Trailer),
    ok = file:datasync(IdxFile),
    ok = file:close(IdxFile),
    {ok, State#state{ index_file=undefined, index_file_pos=undefined }};

archive_nodes(State=#state{ nodes=[#node{level=N, members=[{_,{Pos,_Len}}]}], last_node_pos=Pos })
  when N > 0 ->
    %% Ignore this node, its stack consists of one node with one {pos,len} member
    archive_nodes(State#state{ nodes=[] });

archive_nodes(State) ->
    {ok, State2} = flush_node_buffer(State),
    archive_nodes(State2).


append_node(Level, Key, Value, State=#state{ nodes=[] }) ->
    append_node(Level, Key, Value, State#state{ nodes=[ #node{ level=Level } ] });
append_node(Level, Key, Value, State=#state{ nodes=[ #node{level=Level2 } |_]=Stack })
  when Level < Level2 ->
    append_node(Level, Key, Value, State#state{ nodes=[ #node{ level=(Level2 - 1) } | Stack] });
append_node(Level, Key, Value, #state{ nodes=[ #node{level=Level, members=List, size=NodeSize}=CurrNode | RestNodes ], value_count=VC, tombstone_count=TC, bloom=Bloom }=State) ->
    %% The top-of-stack node is at the level we wish to insert at.

    %% Assert that keys are increasing:
    case List of
        [] ->
            ok;
        [{PrevKey,_}|_] ->
            if
                (Key >= PrevKey) -> ok;
                true ->
                    error_logger:error_msg("keys not ascending ~p < ~p~n", [PrevKey, Key]),
                    exit({badarg, Key})
            end
    end,

    NewSize = NodeSize + hanoidb_util:estimate_node_size_increment(List, Key, Value),

    NewBloom = hanoidb_bloom:add(Key, Bloom),

    {TC1, VC1} =
        case Level of
            0 ->
                case Value of
                    ?TOMBSTONE ->
                        {TC+1, VC};
                    {?TOMBSTONE, _} -> %% Matched when this Value can expire
                        {TC+1, VC};
                    _ ->
                        {TC, VC+1}
                end;
            _ ->
                {TC, VC}
        end,

    NodeMembers = [{Key, Value} | List],
    State2 = State#state{ nodes=[CurrNode#node{members=NodeMembers, size=NewSize} | RestNodes],
                          value_count=VC1, tombstone_count=TC1, bloom=NewBloom },

    case NewSize >= State#state.block_size of
        true ->
            flush_node_buffer(State2);
        false ->
            {ok, State2}
    end.

flush_node_buffer(#state{nodes=[#node{ level=Level, members=NodeMembers }|RestNodes], compress=Compress, index_file_pos=NodePos} = State) ->
    OrderedMembers = lists:reverse(NodeMembers),
    {ok, BlockData} = hanoidb_util:encode_index_node(OrderedMembers, Compress),

    BlockSize = erlang:iolist_size(BlockData),
    Data = [ <<(BlockSize+2):32/unsigned, Level:16/unsigned>> | BlockData ],
    DataSize = BlockSize + 6,

    ok = file:write(State#state.index_file, Data),

    {FirstKey, _} = hd(OrderedMembers),
    append_node(Level + 1, FirstKey, {NodePos, DataSize},
                State#state{ nodes          = RestNodes,
                             index_file_pos = NodePos + DataSize,
                             last_node_pos  = NodePos,
                             last_node_size = DataSize }).
