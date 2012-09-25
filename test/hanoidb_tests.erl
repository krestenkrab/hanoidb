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

-module(hanoidb_tests).

-ifdef(QC_PROPER).

-include("include/hanoidb.hrl").
-include("src/hanoidb.hrl").

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

-record(tree, { elements = dict:new() :: dict() }).
-record(state, { open    = dict:new() :: dict(),
                 closed  = dict:new() :: dict()}).
-define(SERVER, hanoidb_drv).

full_test_() ->
    {setup, spawn, fun () -> ok end, fun (_) -> ok end,
     [
      ?_test(test_tree_simple_1()),
      ?_test(test_tree_simple_2()),
      ?_test(test_tree_simple_4()),
      ?_test(test_tree_simple_5())
     ]}.

longer_test_() ->
    {setup,
     spawn,
     fun () -> ok end,
     fun (_) -> ok end,
     [
       {timeout, 300, ?_test(test_tree())},
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

g_fold_operation() ->
    oneof([{fun (K, V, Acc) -> [{K, V} | Acc] end, []}]).

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
      ++ [ {1500, {call, ?SERVER, get_fail, cmd_get_fail_args(S)}}
        || open_dicts(S)]
      ++ [ {1500, {call, ?SERVER, get_exist, cmd_get_args(S)}}
        || open_dicts(S), open_dicts_with_keys(S)]
      ++ [ {500, {call, ?SERVER, delete_exist, cmd_delete_args(S)}}
           || open_dicts(S), open_dicts_with_keys(S)]
      ++ [ {125, {call, ?SERVER, fold_range, cmd_sync_fold_range_args(S)}}
           || open_dicts(S), open_dicts_with_keys(S)]
     ).

%% Precondition (abstract)
precondition(S, {call, ?SERVER, fold_range, [_Tree, _F, _A0, Range]}) ->
    is_valid_range(Range) andalso open_dicts(S) andalso open_dicts_with_keys(S);
precondition(S, {call, ?SERVER, delete_exist, [_Name, _K]}) ->
    open_dicts(S) andalso open_dicts_with_keys(S);
precondition(S, {call, ?SERVER, get_fail, [_Name, _K]}) ->
    open_dicts(S);
precondition(S, {call, ?SERVER, get_exist, [_Name, _K]}) ->
    open_dicts(S) andalso open_dicts_with_keys(S);
precondition(#state { open = Open }, {call, ?SERVER, put, [Name, _K, _V]}) ->
    dict:is_key(Name, Open);
precondition(#state { open = Open, closed = Closed },
             {call, ?SERVER, open, [Name]}) ->
    (not (dict:is_key(Name, Open))) and (dict:is_key(Name, Closed));
precondition(#state { open = Open, closed = Closed },
             {call, ?SERVER, close, [Name]}) ->
    (dict:is_key(Name, Open)) and (not dict:is_key(Name, Closed)).

is_valid_range(#key_range{ from_key=FromKey, from_inclusive=FromIncl,
                          to_key=ToKey, to_inclusive=ToIncl,
                          limit=Limit })
  when
      (Limit == undefined) orelse (Limit > 0),
      is_binary(FromKey),
      (ToKey == undefined) orelse is_binary(ToKey),
      FromKey =< ToKey,
      is_boolean(FromIncl),
      is_boolean(ToIncl)
      ->
    if (FromKey == ToKey) ->
            (FromIncl == true) and (ToIncl == true);
       true ->
            true
    end;
is_valid_range(_) ->
    false.


%% Next state manipulation (abstract / concrete)
next_state(S, _Res, {call, ?SERVER, fold_range, [_Tree, _F, _A0, _Range]}) ->
    S;
next_state(S, _Res, {call, ?SERVER, get_fail, [_Name, _Key]}) ->
    S;
next_state(S, _Res, {call, ?SERVER, get_exist, [_Name, _Key]}) ->
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
              {call, ?SERVER, fold_range, [Tree, F, A0, Range]}, Result) ->
    #tree { elements = TDict } = dict:fetch(Tree, Open),
    DictResult = lists:sort(dict_range_query(TDict, F, A0, Range)),
    CallResult = lists:sort(Result),
    DictResult == CallResult;
postcondition(_S,
              {call, ?SERVER, get_fail, [_Name, _Key]}, not_found) ->
    true;
postcondition(#state { open = Open },
              {call, ?SERVER, get_exist, [Name, Key]}, {ok, Value}) ->
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
postcondition(_State, _Call, _Result) ->
%    error_logger:error_report([{not_matching_any_postcondition, _State, _Call, _Result}]),
    false.


%% Main property. Running a random set of commands is in agreement
%% with a dict.
prop_dict_agree() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(
               begin
                   hanoidb_drv:start_link(),
                    {History,State,Result} = run_commands(?MODULE, Cmds),
                   hanoidb_drv:stop(),
                   cleanup_test_trees(State),
                    ?WHENFAIL(io:format("History: ~w\nState: ~w\nResult: ~w\n",
                                        [History,State,Result]),
                              Result =:= ok)
                end)).

%% UNIT TESTS
%% ----------------------------------------------------------------------
test_tree_simple_1() ->
    {ok, Tree} = hanoidb:open("simple"),
    ok = hanoidb:put(Tree, <<>>, <<"data", 77:128>>),
    {ok, <<"data", 77:128>>} = hanoidb:get(Tree, <<>>),
    ok = hanoidb:close(Tree).

test_tree_simple_2() ->
    {ok, Tree} = hanoidb:open("simple"),
    ok = hanoidb:put(Tree, <<"ã">>, <<"µ">>),
    {ok, <<"µ">>} = hanoidb:get(Tree, <<"ã">>),
    ok = hanoidb:delete(Tree, <<"ã">>),
    not_found = hanoidb:get(Tree, <<"ã">>),
    ok = hanoidb:close(Tree).

test_tree_simple_4() ->
    Key = <<56,11,62,42,35,163,16,100,9,224,8,228,130,94,198,2,126,117,243,
            1,122,175,79,159,212,177,30,153,71,91,85,233,41,199,190,58,3,
            173,220,9>>,
    Value = <<212,167,12,6,105,152,17,80,243>>,
    {ok, Tree} = hanoidb:open("simple"),
    ok = hanoidb:put(Tree, Key, Value),
    ?assertEqual({ok, Value}, hanoidb:get(Tree, Key)),
    ok = hanoidb:close(Tree).

test_tree_simple_5() ->
    {ok, Tree} = hanoidb:open("simple"),
    ok = hanoidb:put(Tree, <<"foo">>, <<"bar">>, 2),
    {ok, <<"bar">>} = hanoidb:get(Tree, <<"foo">>),
    ok = timer:sleep(3000),
    not_found = hanoidb:get(Tree, <<"foo">>),
    ok = hanoidb:close(Tree).

test_tree() ->
    {ok, Tree} = hanoidb:open("simple2"),
    lists:foldl(fun(N,_) ->
                        ok = hanoidb:put(Tree, <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(2,10000,1)),
%    io:format(user, "INSERT DONE 1~n", []),

    lists:foldl(fun(N,_) ->
                        ok = hanoidb:put(Tree, <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(4000,6000,1)),
%    io:format(user, "INSERT DONE 2~n", []),

    hanoidb:delete(Tree, <<1500:128>>),
%    io:format(user, "DELETE DONE 3~n", []),

    {Time1,{ok,Count1}} = timer:tc(?MODULE, run_fold, [Tree,1000,2000,9]),
%    error_logger:info_msg("time to fold: ~p/sec (time=~p, count=~p)~n", [1000000/(Time1/Count1), Time1/1000000, Count1]),

    {Time2,{ok,Count2}} = timer:tc(?MODULE, run_fold, [Tree,1000,2000,1000]),
%    error_logger:info_msg("time to fold: ~p/sec (time=~p, count=~p)~n", [1000000/(Time2/Count2), Time2/1000000, Count2]),
    ok = hanoidb:close(Tree).

run_fold(Tree,From,To,Limit) ->
    F = fun(<<N:128>>, _Value, {N, C}) ->
                {N + 1, C + 1};
           (<<1501:128>>, _Value, {1500, C}) ->
                {1502, C + 1}
        end,
    {_, Count} = hanoidb:fold_range(Tree, F,
                                    {From, 0},
                                    #key_range{from_key= <<From:128>>, to_key= <<(To+1):128>>, limit=Limit}),
    {ok, Count}.


%% Command processing
%% ----------------------------------------------------------------------
cmd_close_args(#state { open = Open }) ->
    oneof(dict:fetch_keys(Open)).

cmd_put_args(#state { open = Open }) ->
    ?LET({Name, Key, Value},
         {oneof(dict:fetch_keys(Open)), g_key(), g_value()},
         [Name, Key, Value]).


cmd_get_fail_args(#state { open = Open}) ->
    ?LET(Name, g_open_tree(Open),
         ?LET(Key, g_non_existing_key(Name, Open),
              [Name, Key])).

cmd_get_args(#state { open = Open}) ->
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
              [Tree, #key_range{from_key=K1, to_key=K2}])).

cmd_sync_fold_range_args(State) ->
    ?LET([Tree, Range], cmd_sync_range_args(State),
         ?LET({F, Acc0}, g_fold_operation(),
              [Tree, F, Acc0, Range])).

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

dict_range_query(Dict, Fun, Acc0, Range) ->
    KVs = dict_range_query(Dict, Range),
    lists:foldl(fun({K, V}, Acc) ->
                        Fun(K, V, Acc)
                end,
                Acc0,
                KVs).

dict_range_query(Dict, Range) ->
    [{K, V} || {K, V} <- dict:to_list(Dict),
               ?KEY_IN_RANGE(K, Range)].

-endif. %% -ifdef(QC_PROPER).
