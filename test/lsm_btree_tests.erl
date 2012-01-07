-module(lsm_btree_tests).

-ifdef(TEST).
-ifdef(TRIQ).
-include_lib("triq/include/triq.hrl").
-include_lib("triq/include/triq_statem.hrl").
-else.
-include_lib("proper/include/proper.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(PROPER).
-behaviour(proper_statem).
-endif.

-compile(export_all).

-export([command/1, initial_state/0,
         next_state/3, postcondition/3,
         precondition/2]).

-record(state, { open = dict:new(),
                 closed = dict:new() }).
-define(SERVER, lsm_btree_drv).

full_test_() ->
    {setup,
     spawn,
     fun () -> ok end,
     fun (_) -> ok end,
     [
      {timeout, 120, ?_test(test_qc())},
      ?_test(test_tree_simple_1()),
      ?_test(test_tree_simple_2()),
      ?_test(test_tree())
     ]}.

-ifdef(TRIQ).
test_qc() ->
    [?assertEqual(true, triq:module(?MODULE))].
-else.
qc_opts() -> [{numtests, 800}].
test_qc() ->
    [?assertEqual([], proper:module(?MODULE, qc_opts()))].
-endif.

%% Generators
%% ----------------------------------------------------------------------

-define(NUM_TREES, 10).

%% Generate a name for a btree
g_btree_name() ->
    ?LET(I, choose(1,?NUM_TREES),
         btree_name(I)).

%% Pick a name of a non-empty Btree
non_empty_btree(Open) ->
    ?SUCHTHAT(Name, oneof(dict:fetch_keys(Open)),
              dict:size(dict:fetch(Name, Open)) > 0).

btree_name(I) ->
    "Btree_" ++ integer_to_list(I).

%% Statem test
%% ----------------------------------------------------------------------
initial_state() ->
    ClosedBTrees = lists:foldl(fun(N, Closed) ->
                                       dict:store(btree_name(N),
                                                  dict:new(),
                                                  Closed)
                               end,
                               dict:new(),
                               lists:seq(1,?NUM_TREES)),
    #state { closed=ClosedBTrees }.


command(#state { open = Open, closed = Closed } = S) ->
    frequency(
      [ {20, {call, ?SERVER, open, [oneof(dict:fetch_keys(Closed))]}}
        || dict:size(Closed) > 0] ++
      [ {20, {call, ?SERVER, close, [oneof(dict:fetch_keys(Open))]}}
        || dict:size(Open) > 0] ++
      [ {2000, {call, ?SERVER, put, cmd_put_args(S)}}
        || dict:size(Open) > 0] ++
      [ {1500, {call, ?SERVER, lookup_exist, cmd_lookup_args(S)}}
        || dict:size(Open) > 0, count_dicts(Open) > 0] ++
      [ {500, {call, ?SERVER, delete_exist, cmd_delete_args(S)}}
        || dict:size(Open) > 0, count_dicts(Open) > 0 ]).

%% Precondition (abstract)
precondition(#state { open = _Open}, {call, ?SERVER, delete_exist,
                                     [_Name, _K]}) ->
    %% No need to check since we limit this in the command/1 generator
    true;
precondition(#state { open = _Open }, {call, ?SERVER, lookup_exist,
                                       [_Name, _K]}) ->
    %% No need to check since we limit this in the command/1 generator
    true;
precondition(#state { open = Open }, {call, ?SERVER, put, [Name, _K, _V]}) ->
    dict:is_key(Name, Open);
precondition(#state { open = Open, closed = Closed }, {call, ?SERVER, open, [Name]}) ->
    (not (dict:is_key(Name, Open))) and (dict:is_key(Name, Closed));
precondition(#state { open = Open, closed = Closed }, {call, ?SERVER, close, [Name]}) ->
    (dict:is_key(Name, Open)) and (not dict:is_key(Name, Closed)).


%% Next state manipulation (abstract / concrete)
next_state(S, _Res, {call, ?SERVER, lookup_exist, [_Name, _Key]}) ->
    S;
next_state(#state { open = Open} = S, _Res,
           {call, ?SERVER, delete_exist, [Name, Key]}) ->
    S#state { open = dict:update(Name,
                                 fun(Dict) ->
                                         dict:erase(Key, Dict)
                                 end,
                                 Open)};
next_state(#state { open = Open} = S, _Res,
           {call, ?SERVER, put, [Name, Key, Value]}) ->
    S#state { open = dict:update(Name,
                                 fun(Dict) ->
                                         dict:store(Key, Value, Dict)
                                 end,
                                 Open)};
next_state(#state { open = Open, closed=Closed} = S, _Res, {call, ?SERVER, open, [Name]}) ->
    S#state { open   = dict:store(Name, dict:fetch(Name, Closed) , Open),
              closed = dict:erase(Name, Closed) };
next_state(#state { open = Open, closed=Closed} = S, _Res, {call, ?SERVER, close, [Name]}) ->
    S#state { closed = dict:store(Name, dict:fetch(Name, Open) , Closed),
              open   = dict:erase(Name, Open) }.

%% Postcondition check (concrete)
postcondition(#state { open = Open },
              {call, ?SERVER, lookup_exist, [Name, Key]}, {ok, Value}) ->
    dict:fetch(Key, dict:fetch(Name, Open)) == Value;
postcondition(_S, {call, ?SERVER, delete_exist, [_Name, _Key]}, ok) ->
    true;
postcondition(_S, {call, ?SERVER, put, [_Name, _Key, _Value]}, ok) ->
    true;
postcondition(_S, {call, ?SERVER, open, [_Name]}, ok) ->
    true;
postcondition(_S, {call, ?SERVER, close, [_Name]}, ok) ->
    true;
postcondition(_, _, _) ->
    false.


%% Main property. Running a random set of commands is in agreement
%% with a dict.
prop_dict_agree() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   lsm_btree_drv:start_link(),
                    {History,State,Result} = run_commands(?MODULE, Cmds),
                   lsm_btree_drv:stop(),
                   cleanup_test_trees(State),
                    ?WHENFAIL(io:format("History: ~w\nState: ~w\nResult: ~w\n",
                                        [History,State,Result]),
                              Result =:= ok)
                end)).

%% UNIT TESTS
%% ----------------------------------------------------------------------
test_tree_simple_1() ->
    {ok, Tree} = lsm_btree:open("simple"),
    ok = lsm_btree:put(Tree, <<>>, <<"data", 77:128>>),
    {ok, <<"data", 77:128>>} = lsm_btree:lookup(Tree, <<>>),
    ok = lsm_btree:close(Tree).

test_tree_simple_2() ->
    {ok, Tree} = lsm_btree:open("simple"),
    ok = lsm_btree:put(Tree, <<"ã">>, <<"µ">>),
    ok = lsm_btree:delete(Tree, <<"ã">>),
    ok = lsm_btree:close(Tree).

test_tree() ->

%%    application:start(sasl),

    {ok, Tree} = lsm_btree:open("simple"),
    lists:foldl(fun(N,_) ->
                        ok = lsm_btree:put(Tree,
                                               <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(2,10000,1)),

    ok = lsm_btree:close(Tree).

%% Command processing
%% ----------------------------------------------------------------------
cmd_close_args(#state { open = Open }) ->
    oneof(dict:fetch_keys(Open)).

cmd_put_args(#state { open = Open }) ->
    ?LET({Name, Key, Value},
         {oneof(dict:fetch_keys(Open)), binary(), binary()},
         [Name, Key, Value]).


cmd_lookup_args(#state { open = Open}) ->
    ?LET(Name, non_empty_btree(Open),
         ?LET(Key, oneof(dict:fetch_keys(dict:fetch(Name, Open))),
              [Name, Key])).

cmd_delete_args(#state { open = Open}) ->
    ?LET(Name, non_empty_btree(Open),
         ?LET(Key, oneof(dict:fetch_keys(dict:fetch(Name, Open))),
              [Name, Key])).


%% Context management
%% ----------------------------------------------------------------------
cleanup_test_trees(#state { open = Open}) ->
    [cleanup_tree(N) || N <- dict:fetch_keys(Open)].

cleanup_tree(Tree) ->
    {ok, FileNames} = file:list_dir(Tree),
    [ok = file:delete(filename:join([Tree, Fname])) || Fname <- FileNames],
    file:del_dir(Tree).

%% Various Helper routines
%% ----------------------------------------------------------------------

%% @todo optimize this call. You can fast-exit as soon as you know
%% there is a non-empty dict.
count_dicts(Open) ->
    Dicts = [ V || {_, V} <- dict:to_list(Open)],
    lists:sum([dict:size(D) || D <- Dicts]).
