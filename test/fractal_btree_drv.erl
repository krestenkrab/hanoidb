%% @Doc Drive a set of fractal BTrees
-module(fractal_btree_drv).

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([
         lookup_exist/2,
         open/1,
         put/3,
         stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, { btrees = dict:new() % Map from a name to its tree
               }).

%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

call(X) ->
    gen_server:call(?SERVER, X, infinity).

lookup_exist(N, K) ->
    call({lookup_exist, N, K}).

open(N) ->
    call({open, N}).

put(N, K, V) ->
    call({put, N, K, V}).

stop() ->
    call(stop).

%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({open, N}, _, #state { btrees = D} = State) ->
    case fractal_btree:open(N) of
        {ok, Tree} ->
            {reply, ok, State#state { btrees = dict:store(N, Tree, D)}};
        Otherwise ->
            {reply, {error, Otherwise}, State}
    end;
handle_call({put, N, K, V}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    case fractal_btree:put(Tree, K, V) of
        ok ->
            {reply, ok, State};
        Other ->
            {reply, {error, Other}, State}
    end;
handle_call({lookup_exist, N, K}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    Reply = fractal_btree:lookup(Tree, K),
    {reply, Reply, State};
handle_call(stop, _, State) ->
    cleanup_trees(State),
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================

%% @todo directory cleanup
cleanup_trees(#state { btrees = BTs }) ->
    dict:fold(fun(_Name, Tree, ok) ->
                      fractal_btree:close(Tree)
              end,
              ok,
              BTs).


