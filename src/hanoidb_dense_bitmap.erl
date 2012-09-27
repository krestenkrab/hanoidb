-module(hanoidb_dense_bitmap).

-export([new/1, set/2, build/1, unbuild/1, member/2]).
-define(BITS_PER_CELL, 32).

-define(REPR_NAME, dense_bitmap).

new(N) ->
    Tab = ets:new(dense_bitmap, [private, set]),
    Width = 1 + (N-1) div ?BITS_PER_CELL,
    Value = erlang:make_tuple(Width+1, 0, [{1,?REPR_NAME}]),
    ets:insert(Tab, Value),
    {dense_bitmap_ets, N, Width, Tab}.

%% Set a bit.
set(I, {dense_bitmap_ets, _,_, Tab}=DBM) ->
    Cell = 2 + I div ?BITS_PER_CELL,
    BitInCell = I rem ?BITS_PER_CELL,
    Old = ets:lookup_element(Tab, ?REPR_NAME, Cell),
    New = Old bor (1 bsl BitInCell),
    if New =:= Old ->
            ok; % The bit is already set
       true ->
            ets:update_element(Tab, ?REPR_NAME, {Cell,New})
    end,
    DBM.

build({dense_bitmap_ets, _, _, Tab}) ->
    [Row] = ets:lookup(Tab, ?REPR_NAME),
    ets:delete(Tab),
    Row.

unbuild(Row) when element(1,Row)==?REPR_NAME ->
    Tab = ets:new(dense_bitmap, [private, set]),
    ets:insert(Tab, Row),
    {dense_bitmap_ets, undefined, undefined, Tab}.

member(I, Row) when element(1,Row)==?REPR_NAME ->
    Cell = 2 + I div ?BITS_PER_CELL,
    BitInCell = I rem ?BITS_PER_CELL,
    CellValue = element(Cell, Row),
    CellValue band (1 bsl BitInCell) =/= 0;
member(I, {dense_bitmap_ets, _,_, Tab}) ->
    Cell = 2 + I div ?BITS_PER_CELL,
    BitInCell = I rem ?BITS_PER_CELL,
    CellValue = ets:lookup_element(Tab, ?REPR_NAME, Cell),
    CellValue band (1 bsl BitInCell) =/= 0.
