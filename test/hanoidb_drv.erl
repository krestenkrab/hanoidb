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

%% @doc Drive a set of LSM BTrees
-module(hanoidb_drv).

-ifdef(QC_PROPER).

-behaviour(gen_server).

%% API
-export([start_link/0]).

-export([
         delete_exist/2,
         get_exist/2,
         get_fail/2,
         open/1, close/1,
         put/3,
         fold_range/4,
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

get_exist(N, K) ->
    call({get, N, K}).

get_fail(N, K) ->
    call({get, N, K}).

delete_exist(N, K) ->
    call({delete_exist, N, K}).

open(N) ->
    call({open, N}).

close(N) ->
    call({close, N}).

put(N, K, V) ->
    call({put, N, K, V}).

fold_range(T, Fun, Acc0, Range) ->
    call({fold_range, T, Fun, Acc0, Range}).

stop() ->
    call(stop).

%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({open, N}, _, #state { btrees = D} = State) ->
    case hanoidb:open(N) of
        {ok, Tree} ->
            {reply, ok, State#state { btrees = dict:store(N, Tree, D)}};
        Otherwise ->
            {reply, {error, Otherwise}, State}
    end;
handle_call({close, N}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    case hanoidb:close(Tree) of
        ok ->
            {reply, ok, State#state { btrees = dict:erase(N, D)}};
        Otherwise ->
            {reply, {error, Otherwise}, State}
    end;
handle_call({fold_range, Name, Fun, Acc0, Range},
            _From,
            #state { btrees = D } = State) ->
    Tree = dict:fetch(Name, D),
    Result = hanoidb:fold_range(Tree, Fun, Acc0, Range),
    {reply, Result, State};
handle_call({put, N, K, V}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    case hanoidb:put(Tree, K, V) of
        ok ->
            {reply, ok, State};
        Other ->
            {reply, {error, Other}, State}
    end;
handle_call({delete_exist, N, K}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    Reply = hanoidb:delete(Tree, K),
    {reply, Reply, State};
handle_call({get, N, K}, _, #state { btrees = D} = State) ->
    Tree = dict:fetch(N, D),
    Reply = hanoidb:get(Tree, K),
    {reply, Reply, State};
handle_call(stop, _, #state{ btrees = D } = State ) ->
    [ hanoidb:close(Tree) || {_,Tree} <- dict:to_list(D) ],
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

-endif. %% -ifdef(QC_PROPER).
