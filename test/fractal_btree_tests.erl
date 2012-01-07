-module(fractal_btree_tests).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(proper_statem).

-compile(export_all).

-export([command/1, initial_state/0,
         next_state/3, postcondition/3,
         precondition/2]).

-record(state, { open = dict:new(),
                 closed = dict:new() }).
-define(SERVER, fractal_btree_drv).

full_test_() ->
    {setup,
     spawn,
     fun () -> ok end,
     fun (_) -> ok end,
     [
      {timeout, 120, ?_test(test_proper())},
      ?_test(test_tree_simple_1()),
      ?_test(test_tree_simple_2()),
      ?_test(test_tree())
     ]}.

qc_opts() -> [{numtests, 800}].

test_proper() ->
    [?assertEqual([], proper:module(?MODULE, qc_opts()))].

%% Generators
%% ----------------------------------------------------------------------

%% Generate a name for a btree
g_btree_name() ->
    ?LET(I, integer(1,10),
         "Btree_" ++ integer_to_list(I)).

%% Pick a name of a non-empty Btree
non_empty_btree(Open) ->
    ?SUCHTHAT(Name, union(dict:fetch_keys(Open)),
              dict:size(dict:fetch(Name, Open)) > 0).

%% Statem test
%% ----------------------------------------------------------------------
initial_state() ->
    #state { }.


command(#state { open = Open} = S) ->
    frequency(
      [ {20, {call, ?SERVER, open, [g_btree_name()]}} ] ++
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
precondition(#state { open = Open }, {call, ?SERVER, open, [Name]}) ->
    not (dict:is_key(Name, Open)).

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
next_state(#state { open = Open} = S, _Res, {call, ?SERVER, open, [Name]}) ->
    S#state { open = dict:store(Name, dict:new(), Open) }.

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
postcondition(_, _, _) ->
    false.


%% Main property. Running a random set of commands is in agreement
%% with a dict.
prop_dict_agree() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   fractal_btree_drv:start_link(),
                    {History,State,Result} = run_commands(?MODULE, Cmds),
                   fractal_btree_drv:stop(),
                   cleanup_test_trees(State),
                    ?WHENFAIL(io:format("History: ~w\nState: ~w\nResult: ~w\n",
                                        [History,State,Result]),
                              aggregate(command_names(Cmds), Result =:= ok))
                end)).

%% UNIT TESTS
%% ----------------------------------------------------------------------
test_tree_simple_1() ->
    {ok, Tree} = fractal_btree:open("simple"),
    ok = fractal_btree:put(Tree, <<>>, <<"data", 77:128>>),
    {ok, <<"data", 77:128>>} = fractal_btree:lookup(Tree, <<>>),
    ok = fractal_btree:close(Tree).

test_tree_simple_2() ->
    {ok, Tree} = fractal_btree:open("simple"),
    ok = fractal_btree:put(Tree, <<"ã">>, <<"µ">>),
    ok = fractal_btree:delete(Tree, <<"ã">>),
    ok = fractal_btree:close(Tree).

test_tree() ->

%%    application:start(sasl),

    {ok, Tree} = fractal_btree:open("simple"),
    lists:foldl(fun(N,_) ->
                        ok = fractal_btree:put(Tree,
                                               <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(2,10000,1)),

    ok = fractal_btree:close(Tree).

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
