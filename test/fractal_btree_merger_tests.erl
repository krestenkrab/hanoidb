-module(fractal_btree_merger_tests).

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile(export_all).

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


    {Time,{ok,Count}} = timer:tc(fractal_btree_merger2, merge, ["test1", "test2", "test3", 10000]),

    error_logger:info_msg("time to merge: ~p/sec (time=~p, count=~p)~n", [1000000/(Time/Count), Time/1000000, Count]),

    ok = file:delete("test1"),
    ok = file:delete("test2"),
    ok = file:delete("test3"),

    ok.

