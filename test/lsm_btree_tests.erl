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

-record(tree, { elements = dict:new() }).
-record(state, { open = dict:new(),
                 closed = dict:new() }).
-define(SERVER, lsm_btree_drv).

full_test_() ->
    {setup,
     spawn,
     fun () -> ok end,
     fun (_) -> ok end,
     [
      ?_test(test_tree_simple_1()),
      ?_test(test_tree_simple_2()),
      ?_test(test_tree_simple_3()),
      ?_test(test_tree_simple_4()),
      ?_test(test_tree()),
      {timeout, 120, ?_test(test_qc())}
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

%% Generate a key for the Tree
g_key() ->
    binary().

%% Generate a value for the Tree
g_value() ->
    binary().

g_fail_key() ->
    ?LET(T, choose(1,999999999999),
         term_to_binary(T)).

g_open_tree(Open) ->
    oneof(dict:fetch_keys(Open)).

%% Pick a name of a non-empty Btree
g_non_empty_btree(Open) ->
    ?LET(TreesWithKeys, dict:filter(fun(_K, #tree { elements = D}) ->
                                            dict:size(D) > 0
                                    end,
                                    Open),
         oneof(dict:fetch_keys(TreesWithKeys))).

g_existing_key(Name, Open) ->
    #tree { elements = Elems } = dict:fetch(Name, Open),
    oneof(dict:fetch_keys(Elems)).

g_non_existing_key(Name, Open) ->
    ?SUCHTHAT(Key, g_fail_key(),
              begin
                  #tree { elements = D } = dict:fetch(Name, Open),
                  not dict:is_key(Key, D)
              end).

btree_name(I) ->
    "Btree_" ++ integer_to_list(I).

%% Statem test
%% ----------------------------------------------------------------------
initial_state() ->
    ClosedBTrees = lists:foldl(fun(N, Closed) ->
                                       dict:store(btree_name(N),
                                                  #tree { },
                                                  Closed)
                               end,
                               dict:new(),
                               lists:seq(1,?NUM_TREES)),
    #state { closed=ClosedBTrees }.


command(#state { open = Open, closed = Closed } = S) ->
    frequency(
         [ {20, {call, ?SERVER, open, [oneof(dict:fetch_keys(Closed))]}}
        || closed_dicts(S)]
      ++ [ {20, {call, ?SERVER, close, [oneof(dict:fetch_keys(Open))]}}
        || open_dicts(S)]
      ++ [ {2000, {call, ?SERVER, put, cmd_put_args(S)}}
        || open_dicts(S)]
      ++ [ {1500, {call, ?SERVER, lookup_fail, cmd_lookup_fail_args(S)}}
        || open_dicts(S)]
      ++ [ {1500, {call, ?SERVER, lookup_exist, cmd_lookup_args(S)}}
        || open_dicts(S), open_dicts_with_keys(S)]
      ++ [ {500, {call, ?SERVER, delete_exist, cmd_delete_args(S)}}
           || open_dicts(S), open_dicts_with_keys(S)]
      ++ [ {250, {call, ?SERVER, sync_range, cmd_sync_range_args(S)}}
           || open_dicts(S), open_dicts_with_keys(S)]
     ).

%% Precondition (abstract)
precondition(S, {call, ?SERVER, sync_range, [_Tree, _K1, _K2]}) ->
    open_dicts(S) andalso open_dicts_with_keys(S);
precondition(S, {call, ?SERVER, delete_exist, [_Name, _K]}) ->
    open_dicts(S) andalso open_dicts_with_keys(S);
precondition(S, {call, ?SERVER, lookup_fail, [_Name, _K]}) ->
    open_dicts(S);
precondition(S, {call, ?SERVER, lookup_exist, [_Name, _K]}) ->
    open_dicts(S) andalso open_dicts_with_keys(S);
precondition(#state { open = Open }, {call, ?SERVER, put, [Name, _K, _V]}) ->
    dict:is_key(Name, Open);
precondition(#state { open = Open, closed = Closed },
             {call, ?SERVER, open, [Name]}) ->
    (not (dict:is_key(Name, Open))) and (dict:is_key(Name, Closed));
precondition(#state { open = Open, closed = Closed },
             {call, ?SERVER, close, [Name]}) ->
    (dict:is_key(Name, Open)) and (not dict:is_key(Name, Closed)).

%% Next state manipulation (abstract / concrete)
next_state(S, _Res, {call, ?SERVER, sync_range, [_Tree, _K1, _K2]}) ->
    S;
next_state(S, _Res, {call, ?SERVER, lookup_fail, [_Name, _Key]}) ->
    S;
next_state(S, _Res, {call, ?SERVER, lookup_exist, [_Name, _Key]}) ->
    S;
next_state(#state { open = Open} = S, _Res,
           {call, ?SERVER, delete_exist, [Name, Key]}) ->
    S#state { open = dict:update(Name,
                                 fun(#tree { elements = Dict}) ->
                                         #tree { elements =
                                                     dict:erase(Key, Dict)} 
                                 end,
                                 Open)};
next_state(#state { open = Open} = S, _Res,
           {call, ?SERVER, put, [Name, Key, Value]}) ->
    S#state { open = dict:update(
                       Name,
                       fun(#tree { elements = Dict}) ->
                               #tree { elements =
                                           dict:store(Key, Value, Dict) }
                       end,
                       Open)};
next_state(#state { open = Open, closed=Closed} = S,
           _Res, {call, ?SERVER, open, [Name]}) ->
    S#state { open   = dict:store(Name, dict:fetch(Name, Closed) , Open),
              closed = dict:erase(Name, Closed) };
next_state(#state { open = Open, closed=Closed} = S, _Res,
           {call, ?SERVER, close, [Name]}) ->
    S#state { closed = dict:store(Name, dict:fetch(Name, Open) , Closed),
              open   = dict:erase(Name, Open) }.

%% Postcondition check (concrete)
postcondition(#state { open = Open},
              {call, ?SERVER, sync_range, [Tree, K1, K2]}, {ok, Result}) ->
    #tree { elements = TDict } = dict:fetch(Tree, Open),
    lists:sort(dict_range_query(TDict, K1, K2))
        == lists:sort(Result);
postcondition(_S,
              {call, ?SERVER, lookup_fail, [_Name, _Key]}, notfound) ->
    true;
postcondition(#state { open = Open },
              {call, ?SERVER, lookup_exist, [Name, Key]}, {ok, Value}) ->
    #tree { elements = Elems } = dict:fetch(Name, Open),
    dict:fetch(Key, Elems) == Value;
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

test_tree_simple_3() ->
    {ok, Tree} = lsm_btree:open("simple"),
    ok = lsm_btree:put(Tree, <<"X">>, <<"Y">>),
    {ok, Ref} = lsm_btree:sync_range(Tree, <<"X">>, <<"X">>),
    ?assertEqual(ok,
                 receive
                     {fold_done, Ref} -> ok
                 after 1000 -> {error, timeout}
                 end),
    ok = lsm_btree:close(Tree).

test_tree_simple_4() ->
    Key = <<56,11,62,42,35,163,16,100,9,224,8,228,130,94,198,2,126,117,243,
            1,122,175,79,159,212,177,30,153,71,91,85,233,41,199,190,58,3,
            173,220,9>>,
    Value = <<212,167,12,6,105,152,17,80,243>>,
    {ok, Tree} = lsm_btree:open("simple"),
    ok = lsm_btree:put(Tree, Key, Value),
    ?assertEqual({ok, Value}, lsm_btree:lookup(Tree, Key)),
    ok = lsm_btree:close(Tree).

test_tree() ->
    application:start(sasl),

    {ok, Tree} = lsm_btree:open("simple2"),
    lists:foldl(fun(N,_) ->
                        ok = lsm_btree:put(Tree,
                                               <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(2,10000,1)),
    lists:foldl(fun(N,_) ->
                        ok = lsm_btree:put(Tree,
                                               <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(4000,6000,1)),

    lsm_btree:delete(Tree, <<1500:128>>),

    {Time,{ok,Count}} = timer:tc(?MODULE, run_fold, [Tree,1000,2000]),

    error_logger:info_msg("time to fold: ~p/sec (time=~p, count=~p)~n", [1000000/(Time/Count), Time/1000000, Count]),


    ok = lsm_btree:close(Tree).

run_fold(Tree,From,To) ->
    {ok, PID} = lsm_btree:sync_range(Tree, <<From:128>>, <<(To+1):128>>),
    lists:foreach(fun(1500) -> ok;
                     (N) ->
                          receive
                              {fold_result, PID, <<N:128>>,_} -> ok
                          after 100 ->
                                  error_logger:info_msg("timed out on #~p~n", [N])
                          end
                  end,
                  lists:seq(From,To,1)),
    receive
        {fold_done, PID} -> ok
    after 1000 ->
            error_logger:info_msg("timed out on fold_done! ~n", [])
    end,

    %% we should now have no spurious messages
    {messages,[]} = erlang:process_info(self(),messages),

    {ok, To-From}.




%% Command processing
%% ----------------------------------------------------------------------
cmd_close_args(#state { open = Open }) ->
    oneof(dict:fetch_keys(Open)).

cmd_put_args(#state { open = Open }) ->
    ?LET({Name, Key, Value},
         {oneof(dict:fetch_keys(Open)), g_key(), g_value()},
         [Name, Key, Value]).


cmd_lookup_fail_args(#state { open = Open}) ->
    ?LET(Name, g_open_tree(Open),
         ?LET(Key, g_non_existing_key(Name, Open),
              [Name, Key])).

cmd_lookup_args(#state { open = Open}) ->
    ?LET(Name, g_non_empty_btree(Open),
         ?LET(Key, g_existing_key(Name, Open),
              [Name, Key])).

cmd_delete_args(#state { open = Open}) ->
    ?LET(Name, g_non_empty_btree(Open),
         ?LET(Key, g_existing_key(Name, Open),
              [Name, Key])).

cmd_sync_range_args(#state { open = Open }) ->
    ?LET(Tree, g_non_empty_btree(Open),
         ?LET({K1, K2}, {g_existing_key(Tree, Open),
                         g_existing_key(Tree, Open)},
              [Tree, K1, K2])).

%% Context management
%% ----------------------------------------------------------------------
cleanup_test_trees(#state { open = Open, closed = Closed }) ->
    [cleanup_tree(N) || N <- dict:fetch_keys(Open)],
    [cleanup_tree(N) || N <- dict:fetch_keys(Closed)].

cleanup_tree(Tree) ->
    case file:list_dir(Tree) of
        {error, enoent} ->
            ok;
        {ok, FileNames} ->
            [ok = file:delete(filename:join([Tree, Fname]))
             || Fname <- FileNames],
            file:del_dir(Tree)
    end.

%% Various Helper routines
%% ----------------------------------------------------------------------

open_dicts_with_keys(#state { open = Open}) ->
    lists:any(fun({_, #tree { elements = D}}) ->
                         dict:size(D) > 0
              end,
              dict:to_list(Open)).

open_dicts(#state { open = Open}) ->
    dict:size(Open) > 0.

closed_dicts(#state { closed = Closed}) ->
    dict:size(Closed) > 0.

dict_range_query(Dict, LowKey, HighKey) ->
    [{K, V} || {K, V} <- dict:to_list(Dict),
               K >= LowKey,
               K < HighKey].


