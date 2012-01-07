-module(lsm_btree_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    lsm_btree_sup:start_link().

stop(_State) ->
    ok.
