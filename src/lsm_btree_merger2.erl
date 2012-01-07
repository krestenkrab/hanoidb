-module(lsm_btree_merger2).

%%
%% Merging two BTrees
%%

-export([merge/5]).

-define(LOCAL_WRITER, true).

merge(A,B,C, Size, IsLastLevel) ->
    {ok, BT1} = lsm_btree_reader:open(A),
    {ok, BT2} = lsm_btree_reader:open(B),
    case ?LOCAL_WRITER of
        true ->
            {ok, Out} = lsm_btree_writer:init([C, Size]);
        false ->
            {ok, Out} = lsm_btree_writer:open(C, Size)
    end,

    {node, AKVs} = lsm_btree_reader:first_node(BT1),
    {node, BKVs} = lsm_btree_reader:first_node(BT2),

    {ok, Count, Out2} = scan(BT1, BT2, Out, IsLastLevel, AKVs, BKVs, 0),

    %% finish stream tree
    ok = lsm_btree_reader:close(BT1),
    ok = lsm_btree_reader:close(BT2),

    case ?LOCAL_WRITER of
        true ->
            {stop, normal, ok, _} = lsm_btree_writer:handle_call(close, self(), Out2);
        false ->
            ok = lsm_btree_writer:close(Out2)
    end,

    {ok, Count}.


scan(BT1, BT2, Out, IsLastLevel, [], BKVs, Count) ->
    case lsm_btree_reader:next_node(BT1) of
        {node, AKVs} ->
            scan(BT1, BT2, Out, IsLastLevel, AKVs, BKVs, Count);
        end_of_data ->
            scan_only(BT2, Out, IsLastLevel, BKVs, Count)
    end;

scan(BT1, BT2, Out, IsLastLevel, AKVs, [], Count) ->
    case lsm_btree_reader:next_node(BT2) of
        {node, BKVs} ->
            scan(BT1, BT2, Out, IsLastLevel, AKVs, BKVs, Count);
        end_of_data ->
            scan_only(BT1, Out, IsLastLevel, AKVs, Count)
    end;

scan(BT1, BT2, Out, IsLastLevel, [{Key1,Value1}|AT]=AKVs, [{Key2,Value2}|BT]=BKVs, Count) ->
    if Key1 < Key2 ->
            case ?LOCAL_WRITER of
                true ->
                    {noreply, Out2} = lsm_btree_writer:handle_cast({add, Key1, Value1}, Out);
                false ->
                    ok = lsm_btree_writer:add(Out2=Out, Key1, Value1)
            end,

            scan(BT1, BT2, Out2, IsLastLevel, AT, BKVs, Count+1);

       Key2 < Key1 ->
            case ?LOCAL_WRITER of
                true ->
                    {noreply, Out2} = lsm_btree_writer:handle_cast({add, Key2, Value2}, Out);
                false ->
                    ok = lsm_btree_writer:add(Out2=Out, Key2, Value2)
            end,
            scan(BT1, BT2, Out2, IsLastLevel, AKVs, BT, Count+1);

       (delete =:= Value2) and (true =:= IsLastLevel) ->
            scan(BT1, BT2, Out, IsLastLevel, AT, BT, Count);

       true ->
            case ?LOCAL_WRITER of
                true ->
                    {noreply, Out2} = lsm_btree_writer:handle_cast({add, Key2, Value2}, Out);
                false ->
                    ok = lsm_btree_writer:add(Out2=Out, Key2, Value2)
            end,
            scan(BT1, BT2, Out2, IsLastLevel, AT, BT, Count+1)
    end.

scan_only(BT, Out, IsLastLevel, [], Count) ->
    case lsm_btree_reader:next_node(BT) of
        {node, KVs} ->
            scan_only(BT, Out, IsLastLevel, KVs, Count);
        end_of_data ->
            {ok, Count, Out}
    end;

scan_only(BT, Out, true, [{_,delete}|Rest], Count) ->
    scan_only(BT, Out, true, Rest, Count);

scan_only(BT, Out, IsLastLevel, [{Key,Value}|Rest], Count) ->
    case ?LOCAL_WRITER of
        true ->
            {noreply, Out2} = lsm_btree_writer:handle_cast({add, Key, Value}, Out);
        false ->
            ok = lsm_btree_writer:add(Out2=Out, Key, Value)
    end,
    scan_only(BT, Out2, IsLastLevel, Rest, Count+1).
