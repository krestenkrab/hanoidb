-module(fractal_btree_tests).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile(export_all).

simple_test() ->

    {ok, BT} = fractal_btree_writer:open("testdata"),
    ok = fractal_btree_writer:add(BT, <<"A">>, <<"Avalue">>),
    ok = fractal_btree_writer:add(BT, <<"B">>, <<"Bvalue">>),
    ok = fractal_btree_writer:close(BT),

    {ok, IN} = fractal_btree_reader:open("testdata"),
    {ok, <<"Avalue">>} = fractal_btree_reader:lookup(IN, <<"A">>),
    ok = fractal_btree_reader:close(IN),

    ok = file:delete("testdata").


simple1_test() ->

    {ok, BT} = fractal_btree_writer:open("testdata"),

    Max = 30*1024,
    Seq = lists:seq(0, Max),

    {Time1,_} = timer:tc(
                  fun() ->
                          lists:foreach(
                            fun(Int) ->
                                    ok = fractal_btree_writer:add(BT, <<Int:128>>, <<"valuevalue/", Int:128>>)
                            end,
                            Seq),
                          ok = fractal_btree_writer:close(BT)
                  end,
                  []),

    error_logger:info_msg("time to insert: ~p/sec~n", [1000000/(Time1/Max)]),

    {ok, IN} = fractal_btree_reader:open("testdata"),
    {ok, <<"valuevalue/", 2048:128>>} = fractal_btree_reader:lookup(IN, <<2048:128>>),


    {Time2,Count} = timer:tc(
                      fun() -> fractal_btree_reader:fold(fun(Key, <<"valuevalue/", Key/binary>>, N) ->
                                                         N+1
                                                 end,
                                                 0,
                                                 IN)
                      end,
                      []),

    error_logger:info_msg("time to scan: ~p/sec~n", [1000000/(Time2/Max)]),

    Max = Count-1,


    ok = fractal_btree_reader:close(IN),

    ok = file:delete("testdata").


merge_test() ->

    {ok, BT1} = fractal_btree_writer:open("test1"),
    lists:foldl(fun(N,_) ->
                        ok = fractal_btree_writer:add(BT1, <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(1,10000,2)),
    ok = fractal_btree_writer:close(BT1),


    {ok, BT2} = fractal_btree_writer:open("test2"),
    lists:foldl(fun(N,_) ->
                        ok = fractal_btree_writer:add(BT2, <<N:128>>, <<"data",N:128>>)
                end,
                ok,
                lists:seq(2,5001,1)),
    ok = fractal_btree_writer:close(BT2),


    {Time,{ok,Count}} = timer:tc(fractal_btree_merger, merge, ["test1", "test2", "test3", 10000]),

    error_logger:info_msg("time to merge: ~p/sec (time=~p, count=~p)~n", [1000000/(Time/Count), Time/1000000, Count]),

    
    ok = file:delete("test1"),
    ok = file:delete("test2"),
    ok = file:delete("test3"),

    ok.



