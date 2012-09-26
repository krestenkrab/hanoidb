-module(hanoidb_sparse_bitmap).
-export([new/1, set/2, member/2]).

-define(REPR_NAME, sparse_bitmap).

new(Bits) when is_integer(Bits), Bits>0 ->
    {?REPR_NAME, Bits, []}.

set(N, {?REPR_NAME, Bits, Tree}) ->
    {?REPR_NAME, Bits, set_to_tree(N, 1 bsl (Bits-1), Tree)}.

set_to_tree(N, HighestBit, Mask) when HighestBit<32 ->
    Nbit = 1 bsl N,
    case Mask of
        []-> Nbit;
        _ -> Nbit bor Mask
    end;
set_to_tree(N, _HighestBit, []) -> N;
set_to_tree(N, HighestBit, [TLo|THi]) ->
    pushdown(N, HighestBit, TLo, THi);
set_to_tree(N, _HighestBit, N) -> N;
set_to_tree(N, HighestBit, M) when is_integer(M) ->
    set_to_tree(N, HighestBit, pushdown(M, HighestBit, [], [])).

pushdown(N, HighestBit, TLo, THi) ->
    NHigh = N band HighestBit,
    if NHigh =:= 0 -> [set_to_tree(N, HighestBit bsr 1, TLo) | THi];
       true        -> [TLo | set_to_tree(N bxor NHigh, HighestBit bsr 1, THi)]
    end.

member(N, {?REPR_NAME, Bits, Tree}) ->
    member_in_tree(N, 1 bsl (Bits-1), Tree).

member_in_tree(_N, _HighestBit, []) -> false;
member_in_tree(N, HighestBit, Mask) when HighestBit<32 ->
    Nbit = 1 bsl N,
    Nbit band Mask > 0;
member_in_tree(N, _HighestBit, M) when is_integer(M) -> N =:= M;
member_in_tree(N, HighestBit, [TLo|THi]) ->
    NHigh = N band HighestBit,
    if NHigh =:= 0 -> member_in_tree(N, HighestBit bsr 1, TLo);
       true        -> member_in_tree(N bxor NHigh, HighestBit bsr 1, THi)
    end.
