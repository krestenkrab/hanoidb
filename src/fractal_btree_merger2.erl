-module(fractal_btree_merger2).

%%
%% Naive Merge of two b-trees.  A better implementation should iterate leafs, not KV's
%%

-export([merge/4]).

-define(LOCAL_WRITER, true).

merge(A,B,C, Size) ->
    {ok, BT1} = fractal_btree_reader:open(A),
    {ok, BT2} = fractal_btree_reader:open(B),
    case ?LOCAL_WRITER of
        true ->
            {ok, Out} = fractal_btree_writer:init([C, Size]);
        false ->
            {ok, Out} = fractal_btree_writer:open(C, Size)
    end,

    {node, AKVs} = fractal_btree_reader:first_node(BT1),
    {node, BKVs} = fractal_btree_reader:first_node(BT2),

    {ok, Count, Out2} = scan(BT1, BT2, Out, AKVs, BKVs, 0),

    %% finish stream tree
    ok = fractal_btree_reader:close(BT1),
    ok = fractal_btree_reader:close(BT2),

    case ?LOCAL_WRITER of
        true ->
            {stop, normal, ok, _} = fractal_btree_writer:handle_call(close, self(), Out2);
        false ->
            ok = fractal_btree_writer:close(Out2)
    end,

    {ok, Count}.


scan(BT1, BT2, Out, [], BKVs, Count) ->
    case fractal_btree_reader:next_node(BT1) of
        {node, AKVs} ->
            scan(BT1, BT2, Out, AKVs, BKVs, Count);
        end_of_data ->
            scan_only(BT2, Out, BKVs, Count)
    end;

scan(BT1, BT2, Out, AKVs, [], Count) ->
    case fractal_btree_reader:next_node(BT2) of
        {node, BKVs} ->
            scan(BT1, BT2, Out, AKVs, BKVs, Count);
        end_of_data ->
            scan_only(BT1, Out, AKVs, Count)
    end;

scan(BT1, BT2, Out, [{Key1,Value1}|AT]=AKVs, [{Key2,Value2}|BT]=BKVs, Count) ->
    if Key1 < Key2 ->
            case ?LOCAL_WRITER of
                true ->
                    {noreply, Out2} = fractal_btree_writer:handle_cast({add, Key1, Value1}, Out);
                false ->
                    ok = fractal_btree_writer:add(Out2=Out, Key1, Value1)
            end,

            scan(BT1, BT2, Out2, AT, BKVs, Count+1);

       Key2 < Key1 ->
            case ?LOCAL_WRITER of
                true ->
                    {noreply, Out2} = fractal_btree_writer:handle_cast({add, Key2, Value2}, Out);
                false ->
                    ok = fractal_btree_writer:add(Out2=Out, Key2, Value2)
            end,
            scan(BT1, BT2, Out2, AKVs, BT, Count+1);

       Key1 == Key2 ->
            %% TODO: eliminate tombstones, right now they just bubble down
            case ?LOCAL_WRITER of
                true ->
                    {noreply, Out2} = fractal_btree_writer:handle_cast({add, Key2, Value2}, Out);
                false ->
                    ok = fractal_btree_writer:add(Out2=Out, Key2, Value2)
            end,
            scan(BT1, BT2, Out2, AT, BT, Count+1)
    end.

scan_only(BT, Out, [], Count) ->
    case fractal_btree_reader:next_node(BT) of
        {node, KVs} ->
            scan_only(BT, Out, KVs, Count);
        end_of_data ->
            {ok, Count, Out}
    end;

scan_only(BT, Out, [{Key,Value}|Rest], Count) ->
    case ?LOCAL_WRITER of
        true ->
            {noreply, Out2} = fractal_btree_writer:handle_cast({add, Key, Value}, Out);
        false ->
            ok = fractal_btree_writer:add(Out2=Out, Key, Value)
    end,
    scan_only(BT, Out2, Rest, Count+1).
