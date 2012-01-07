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
