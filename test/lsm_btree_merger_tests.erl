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

-module(lsm_btree_merger_tests).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile(export_all).

merge_test() ->

    file:delete("test1"),
    file:delete("test2"),
    file:delete("test3"),

    {ok, BT1} = lsm_btree_writer:open("test1"),
    lists:foldl(fun(N,_) ->
                        ok = lsm_btree_writer:add(BT1, <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(1,10000,2)),
    ok = lsm_btree_writer:close(BT1),


    {ok, BT2} = lsm_btree_writer:open("test2"),
    lists:foldl(fun(N,_) ->
                        ok = lsm_btree_writer:add(BT2, <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(2,5001,1)),
    ok = lsm_btree_writer:close(BT2),


    self() ! {step, {self(), none}, 2000000000},
    {Time,{ok,Count}} = timer:tc(lsm_btree_merger, merge, ["test1", "test2", "test3", 10000, true]),

    error_logger:info_msg("time to merge: ~p/sec (time=~p, count=~p)~n", [1000000/(Time/Count), Time/1000000, Count]),

    ok.

