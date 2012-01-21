%% @Doc Drive a set of lsm BTrees
-module(lsm_btree_drv).

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([
         delete_exist/2,
         lookup_exist/2,
         lookup_fail/2,
         open/1, close/1,
         put/3,
         sync_range/3,
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
    call({lookup, N, K}).

lookup_fail(N, K) ->
    call({lookup, N, K}).

delete_exist(N, K) ->
    call({delete_exist, N, K}).

open(N) ->
    call({open, N}).

close(N) ->
    call({close, N}).

put(N, K, V) ->
    call({put, N, K, V}).

sync_range(T, LK, HK) ->
    call({sync_range, T, LK, HK}).

stop() ->
    call(stop).

%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({open, N}, _, #state { btrees = D} = State) ->
    case lsm_btree:open(N) of
        {ok, Tree} ->
            {reply, ok, State#state { btrees = dict:store(N, Tree, D)}};
        Otherwise ->
            {reply, {error, Otherwise}, State}
    end;
handle_call({close, N}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    case lsm_btree:close(Tree) of
        ok ->
            {reply, ok, State#state { btrees = dict:erase(N, D)}};
        Otherwise ->
            {reply, {error, Otherwise}, State}
    end;
handle_call({sync_range, Name, LoKey, HiKey}, _From,
            #state { btrees = D} = State) ->
    Tree = dict:fetch(Name, D),
    {ok, Ref} = lsm_btree:sync_range(Tree, LoKey, HiKey),
    Result = sync_range_gather(Ref),
    {reply, Result, State};
handle_call({put, N, K, V}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    case lsm_btree:put(Tree, K, V) of
        ok ->
            {reply, ok, State};
        Other ->
            {reply, {error, Other}, State}
    end;
handle_call({delete_exist, N, K}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    Reply = lsm_btree:delete(Tree, K),
    {reply, Reply, State};
handle_call({lookup, N, K}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    Reply = lsm_btree:lookup(Tree, K),
    {reply, Reply, State};
handle_call(stop, _, #state{ btrees = D } = State ) ->
    [ lsm_btree:close(Tree) || {_,Tree} <- dict:to_list(D) ],
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
                      lsm_btree:close(Tree)
              end,
              ok,
              BTs).


sync_range_gather(Ref) ->
    sync_range_gather(Ref, []).

sync_range_gather(Ref, Acc) ->
    receive
        {fold_result, Ref, K, V} ->
            sync_range_gather(Ref, [{K, V} | Acc]);
        {fold_done, Ref} ->
            {ok, Acc}
    after 3000 ->
            {error, timeout}
    end.
