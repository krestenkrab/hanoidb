-module(fractal_btree).

-behavior(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-export([open/1, close/1, lookup/2, delete/2, put/3]).



-include_lib("kernel/include/file.hrl").

-record(state, { top, nursery = gb_trees:empty(), dir }).

%% smallest levels are 32 entries
-define(TOP_LEVEL, 5).
-define(TOP_SIZE, (1 bsl ?TOP_LEVEL)).

%% PUBLIC API

open(Dir) ->
    gen_server:start(?MODULE, [Dir], []).

close(Ref) ->
    gen_server:call(Ref, close).

lookup(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {lookup, Key}).

delete(Ref,Key) when is_binary(Key) ->
    gen_server:call(Ref, {delete, Key}).

put(Ref,Key,Value) when is_binary(Key), is_binary(Value) ->
    gen_server:call(Ref, {put, Key, Value}).



init([Dir]) ->

    case file:read_file_info(Dir) of
        {ok, #file_info{ type=directory }} ->
            {ok, TopLevel} = open_levels(Dir);
            %% TODO: recover nursery

        {error, E} when E =:= enoent ->
            ok = file:make_dir(Dir),
            {ok, TopLevel} = fractal_btree_level:open(Dir, ?TOP_LEVEL, undefined)
    end,

    {ok, #state{ top=TopLevel, dir=Dir }}.



open_levels(Dir) ->
    {ok, Files} = file:list_dir(Dir),

    %% parse file names and find max level
    {MinLevel,MaxLevel} =
        lists:foldl(fun(FileName, {MinLevel,MaxLevel}) ->
                            case parse_level(FileName) of
                                {ok, Level} ->
                                    { erlang:min(MinLevel, Level),
                                      erlang:max(MaxLevel, Level) };
                                _ ->
                                    {MinLevel,MaxLevel}
                            end
                    end,
                    {?TOP_LEVEL, ?TOP_LEVEL},
                    Files),

%    error_logger:info_msg("found level files ... {~p,~p}~n", [MinLevel, MaxLevel]),

    %% remove old nursery file
    file:delete(filename:join(Dir,"nursery.data")),

    TopLevel =
        lists:foldl( fun(LevelNo, Prev) ->
                             {ok, Level} = fractal_btree_level:open(Dir,LevelNo,Prev),
                             Level
                     end,
                     undefined,
                     lists:seq(MaxLevel, MinLevel, -1)),

    {ok, TopLevel}.

parse_level(FileName) ->
    case re:run(FileName, "^[^\\d]+-(\\d+)\\.data$", [{capture,all_but_first,list}]) of
        {match,[StringVal]} ->
            {ok, list_to_integer(StringVal)};
        _ ->
            nomatch
    end.


handle_info(Info,State) ->
    error_logger:error_msg("Unknown info ~p~n", [Info]),
    {stop,bad_msg,State}.

handle_cast(Info,State) ->
    error_logger:error_msg("Unknown cast ~p~n", [Info]),
    {stop,bad_msg,State}.


%% premature delete -> cleanup
terminate(_Reason,_State) ->
    % error_logger:info_msg("got terminate(~p,~p)~n", [Reason,State]),
    % flush_nursery(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



handle_call({put, Key, Value}, _From, State) when is_binary(Key), is_binary(Value) ->
    {ok, State2} = do_put(Key, Value, State),
    {reply, ok, State2};

handle_call({delete, Key}, _From, State) when is_binary(Key) ->
    {ok, State2} = do_put(Key, deleted, State),
    {reply, ok, State2};

handle_call({lookup, Key}, _From, State=#state{ top=Top, nursery=Nursery } ) when is_binary(Key) ->
    case gb_trees:lookup(Key, Nursery) of
        {value, deleted} ->
            {reply, notfound, State};
        {value, Value} when is_binary(Value) ->
            {reply, {ok, Value}, State};
        none ->
            Reply = fractal_btree_level:lookup(Top, Key),
            {reply, Reply, State}
    end;

handle_call(close, _From, State) ->
    {ok, State2} = flush_nursery(State),
    {stop, normal, ok, State2}.


do_put(Key, Value, State=#state{ nursery=Tree }) ->
    Tree2 = gb_trees:enter(Key, Value, Tree),
    TreeSize = gb_trees:size(Tree2),

    if TreeSize >= (1 bsl ?TOP_LEVEL) ->
            flush_nursery(State#state{ nursery=Tree2 });
       true ->
            {ok, State#state{ nursery=Tree2 }}
    end.

flush_nursery(State=#state{nursery=Tree, top=Top}) ->
    TreeSize = gb_trees:size( Tree ),
    if TreeSize > 0 ->
%            error_logger:info_msg("flushing to top=~p, alive=~p~n", [Top, erlang:is_process_alive(Top)]),
            FileName = filename:join(State#state.dir, "nursery.data"),
            {ok, BT} = fractal_btree_writer:open(FileName, (1 bsl ?TOP_LEVEL)),
            lists:foreach( fun({Key2,Value2}) ->
                                   ok = fractal_btree_writer:add(BT, Key2, Value2)
                           end,
                           gb_trees:to_list(Tree)),
            ok = fractal_btree_writer:close(BT),
            ok = fractal_btree_level:inject(Top, FileName),
            {error, enoent} = file:read_file_info(FileName),
            {ok, State#state{ nursery=gb_trees:empty() } };
       true ->
            {ok, State}
    end.
