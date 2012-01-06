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
     [{timeout, 120, ?_test(test_proper())},
      ?_test(test_tree())]}.

qc_opts() -> [{numtests, 400}].

test_proper() ->
    [?assertEqual([], proper:module(?MODULE, qc_opts()))].


initial_state() ->
    #state { }.

g_btree_name() ->
    ?LET(I, integer(1,10),
         "Btree_" ++ integer_to_list(I)).

cmd_close_args(#state { open = Open }) ->
    oneof(dict:fetch_keys(Open)).

cmd_put_args(#state { open = Open }) ->
    ?LET({Name, Key, Value},
         {oneof(dict:fetch_keys(Open)), binary(), binary()},
         [Name, Key, Value]).

command(#state { open = Open} = S) ->
    frequency(
      [ {100, {call, ?SERVER, open, [g_btree_name()]}} ] ++
      [ {2000, {call, ?SERVER, put, cmd_put_args(S)}}
          || dict:size(Open) > 0]).

precondition(#state { open = Open }, {call, ?SERVER, put, [Name, K, V]}) ->
    dict:is_key(Name, Open);
precondition(#state { open = Open }, {call, ?SERVER, open, [Name]}) ->
    not (dict:is_key(Name, Open)).

next_state(#state { open = Open} = S, _Res,
           {call, ?SERVER, put, [Name, Key, Value]}) ->
    S#state { open = dict:update(Name,
                                 fun(Dict) ->
                                         dict:store(Key, Value, Dict)
                                 end,
                                 Open)};
next_state(#state { open = Open} = S, _Res, {call, ?SERVER, open, [Name]}) ->
    S#state { open = dict:store(Name, dict:new(), Open) }.

postcondition(_S, {call, ?SERVER, put, [_Name, _Key, _Value]}, ok) ->
    true;
postcondition(_S, {call, ?SERVER, open, [_Name]}, ok) ->
    true;
postcondition(_, _, _) ->
    false.


prop_dict_agree() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   fractal_btree_drv:start_link(),
                    {History,State,Result} = run_commands(?MODULE, Cmds),
                   fractal_btree_drv:stop(),
                    ?WHENFAIL(io:format("History: ~w\nState: ~w\nResult: ~w\n",
                                        [History,State,Result]),
                              aggregate(command_names(Cmds), Result =:= ok))
                end)).

%% ----------------------------------------------------------------------



%% UNIT TESTS -----------------------------------------------------------------


test_tree() ->

    application:start(sasl),

    {ok, Tree} = fractal_btree:open("simple"),
    lists:foldl(fun(N,_) ->
                        ok = fractal_btree:put(Tree, <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(2,10000,1)),

    ok = fractal_btree:close(Tree).


