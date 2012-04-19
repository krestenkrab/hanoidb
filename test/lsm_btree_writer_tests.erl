%% ----------------------------------------------------------------------------
%%
%% lsm_btree: LSM-trees (Log-Structured Merge Trees) Indexed Storage
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

-module(lsm_btree_writer_tests).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile(export_all).

simple_test() ->

    {ok, BT} = lsm_btree_writer:open("testdata"),
    ok = lsm_btree_writer:add(BT, <<"A">>, <<"Avalue">>),
    ok = lsm_btree_writer:add(BT, <<"B">>, <<"Bvalue">>),
    ok = lsm_btree_writer:close(BT),

    {ok, IN} = lsm_btree_reader:open("testdata"),
    {ok, <<"Avalue">>} = lsm_btree_reader:lookup(IN, <<"A">>),
    ok = lsm_btree_reader:close(IN),

    ok = file:delete("testdata").


simple1_test() ->

    {ok, BT} = lsm_btree_writer:open("testdata"),

    Max = 30*1024,
    Seq = lists:seq(0, Max),

    {Time1,_} = timer:tc(
                  fun() ->
                          lists:foreach(
                            fun(Int) ->
                                    ok = lsm_btree_writer:add(BT, <<Int:128>>, <<"valuevalue/", Int:128>>)
                            end,
                            Seq),
                          ok = lsm_btree_writer:close(BT)
                  end,
                  []),

    error_logger:info_msg("time to insert: ~p/sec~n", [1000000/(Time1/Max)]),

    {ok, IN} = lsm_btree_reader:open("testdata"),
    {ok, <<"valuevalue/", 2048:128>>} = lsm_btree_reader:lookup(IN, <<2048:128>>),


    {Time2,Count} = timer:tc(
                      fun() -> lsm_btree_reader:fold(fun(Key, <<"valuevalue/", Key/binary>>, N) ->
                                                         N+1
                                                 end,
                                                 0,
                                                 IN)
                      end,
                      []),

    error_logger:info_msg("time to scan: ~p/sec~n", [1000000/(Time2/Max)]),

    Max = Count-1,


    ok = lsm_btree_reader:close(IN),

    ok = file:delete("testdata").
