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

-module(hanoidb_writer_tests).

-ifdef(QC_PROPER).

-ifdef(TEST).
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
-endif.

-include("include/hanoidb.hrl").

-compile(export_all).

simple_test() ->

    file:delete("testdata"),
    {ok, BT} = hanoidb_writer:open("testdata"),
    ok = hanoidb_writer:add(BT, <<"A">>, <<"Avalue">>),
    ok = hanoidb_writer:add(BT, <<"B">>, <<"Bvalue">>),
    ok = hanoidb_writer:close(BT),

    {ok, IN} = hanoidb_reader:open("testdata"),
    {ok, <<"Avalue">>} = hanoidb_reader:lookup(IN, <<"A">>),
    ok = hanoidb_reader:close(IN),

    ok = file:delete("testdata").


simple1_test() ->

    file:delete("testdata"),
    {ok, BT} = hanoidb_writer:open("testdata", [{block_size, 1024},{expiry_secs, 0}]),

    Max = 1024,
    Seq = lists:seq(0, Max),

    {Time1,_} = timer:tc(
                  fun() ->
                          lists:foreach(
                            fun(Int) ->
                                    ok = hanoidb_writer:add(BT, <<Int:128>>, <<"valuevalue/", Int:128>>)
                            end,
                            Seq),
                          ok = hanoidb_writer:close(BT)
                  end,
                  []),

%    error_logger:info_msg("time to insert: ~p/sec~n", [1000000/(Time1/Max)]),

    {ok, IN} = hanoidb_reader:open("testdata", [{expiry_secs,0}]),
    Middle = Max div 2,
    {ok, <<"valuevalue/", Middle:128>>} = hanoidb_reader:lookup(IN, <<Middle:128>>),


    {Time2,Count} = timer:tc(
                      fun() -> hanoidb_reader:fold(fun(Key, <<"valuevalue/", Key/binary>>, N) ->
                                                         N+1
                                                 end,
                                                 0,
                                                 IN)
                      end,
                      []),

%    error_logger:info_msg("time to scan: ~p/sec~n", [1000000/(Time2/Max)]),

    Max = Count-1,

    {Time3,{done,Count2}} = timer:tc(
                      fun() -> hanoidb_reader:range_fold(fun(Key, <<"valuevalue/", Key/binary>>, N) ->
                                                               N+1
                                                       end,
                                                       0,
                                                       IN,
                                                      #key_range{ from_key= <<>>, to_key=undefined })
                      end,
                      []),

%    error_logger:info_msg("time to range_fold: ~p/sec~n", [1000000/(Time3/Max)]),

%    error_logger:info_msg("count2=~p~n", [Count2]),

    Max = Count2-1,

    ok = hanoidb_reader:close(IN).

-endif. %% -ifdef(QC_PROPER).
