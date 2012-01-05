-module(fractal_btree_writer).

%%
%% Streaming btree writer. Accepts only monotonically increasing keys for put.
%%

%% TODO: add a bloom filter to the file

-define(NODE_SIZE, 2*1024).

-behavior(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([open/1, open/2, add/3,close/1]).

-record(node, { level, members=[], size=0 }).

-record(state, { index_file,
                 index_file_pos,

                 last_node_pos :: pos_integer(),
                 last_node_size :: pos_integer(),

                 nodes = [] :: [ #node{} ],

                 name :: string(),

                 bloom
               }).


%%% PUBLIC API

open(Name,Size) ->
    gen_server:start_link(?MODULE, [Name,Size], []).


open(Name) ->
    gen_server:start_link(?MODULE, [Name,2048], []).


add(Ref,Key,Data) ->
    gen_server:cast(Ref, {add, Key, Data}).

close(Ref) ->
    gen_server:call(Ref, close, infinity).

%%%


init([Name,Size]) ->

%    io:format("got name: ~p~n", [Name]),

    case file:open( fractal_btree_util:index_file_name(Name),
                               [raw, exclusive, write, delayed_write]) of
        {ok, IdxFile} ->
            {ok, BloomFilter} = ebloom:new(erlang:min(Size,16#ffffffff), 0.01, 123),
            {ok, #state{ name=Name,
                         index_file_pos=0, index_file=IdxFile,
                         bloom = BloomFilter
                       }};
        {error, _}=Error ->
            error_logger:error_msg("fractal_btree_writer cannot open ~p: ~p~n", [Name, Error]),
            {stop, Error}
    end.


handle_cast({add, Key, Data}, State) when is_binary(Key), is_binary(Data) ->
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
    file:delete( fractal_btree_util:index_file_name(State#state.name) ).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%%% INTERNAL FUNCTIONS



flush_nodes(#state{ nodes=[], last_node_pos=LastNodePos, last_node_size=LastNodeSize, bloom=Ref }=State) ->

    Bloom = zlib:zip(ebloom:serialize(Ref)),
    BloomSize = byte_size(Bloom),

    Trailer = << 0:32, Bloom/binary, BloomSize:32/unsigned,  LastNodePos:64/unsigned >>,
    IdxFile = State#state.index_file,

    ok = file:write(IdxFile, Trailer),
    ok = file:close(IdxFile),

    {ok, State#state{ index_file=undefined }};

flush_nodes(State=#state{ nodes=[#node{level=N, members=[_]}] }) when N>0 ->
    flush_nodes(State#state{ nodes=[] });

flush_nodes(State) ->
    {ok, State2} = close_node(State),
    flush_nodes(State2).

add_record(Level, Key, Value, #state{ nodes=[ #node{level=Level, members=List, size=NodeSize}=CurrNode |RestNodes] }=State) ->

    %% assert that keys are increasing
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

    NewSize = NodeSize + fractal_btree_util:estimate_node_size_increment(List, Key, Value),

    ebloom:insert( State#state.bloom, Key ),

    NodeMembers = [{Key,Value} | List],
    if
       NewSize >= ?NODE_SIZE ->
            close_node(State#state{ nodes=[CurrNode#node{ members=NodeMembers, size=NewSize} | RestNodes] });
        true ->
            {ok, State#state{ nodes=[ CurrNode#node{ members=NodeMembers, size=NewSize } | RestNodes ] }}
    end;

add_record(Level, Key, Value, #state{ nodes=Nodes }=State) ->
    add_record(Level, Key, Value, State#state{ nodes = [ #node{ level=Level, members=[] } | Nodes ] }).



close_node(#state{nodes=[#node{ level=Level, members=NodeMembers }|RestNodes]} = State) ->
    OrderedMembers = lists:reverse(NodeMembers),
    {ok, DataSize, Data} = fractal_btree_util:encode_index_node(Level, OrderedMembers),
    NodePos = State#state.index_file_pos,
    ok = file:write(State#state.index_file, Data),

    {FirstKey, _} = hd(OrderedMembers),
    add_record(Level+1, FirstKey, {NodePos, DataSize},
               State#state{ nodes          = RestNodes,
                            index_file_pos = NodePos + DataSize,
                            last_node_pos  = NodePos,
                            last_node_size = DataSize
                          }).
