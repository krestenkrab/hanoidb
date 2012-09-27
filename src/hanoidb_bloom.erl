%% The contents of this file are subject to the Erlang Public License, Version
%% 1.1, (the "License"); you may not use this file except in compliance with
%% the License. You should have received a copy of the Erlang Public License
%% along with this software. If not, it can be retrieved via the world wide web
%% at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS" basis,
%% WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
%% the specific language governing rights and limitations under the License.

%% Based on: Scalable Bloom Filters
%%   Paulo Sérgio Almeida, Carlos Baquero, Nuno Preguiça, David Hutchison
%%   Information Processing Letters
%%   Volume 101, Issue 6, 31 March 2007, Pages 255-261
%%
%% Provides scalable bloom filters that can grow indefinitely while ensuring a
%% desired maximum false positive probability. Also provides standard
%% partitioned bloom filters with a maximum capacity. Bit arrays are
%% dimensioned as a power of 2 to enable reusing hash values across filters
%% through bit operations. Double hashing is used (no need for enhanced double
%% hashing for partitioned bloom filters).

%% Modified slightly by Justin Sheehy to make it a single file (incorporated
%% the array-based bitarray internally).
-module(hanoidb_bloom).
-author("Paulo Sergio Almeida <psa@di.uminho.pt>").

-export([sbf/1, sbf/2, sbf/3, sbf/4,
         bloom/1, bloom/2,
         member/2, add/2,
         size/1, capacity/1,
         encode/1, decode/1]).
-import(math, [log/1, pow/2]).

-ifdef(TEST).
-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(W, 27).

-type bitmask() :: array() | any().

-record(bloom, {
    e     :: float(),              % error probability
    n     :: non_neg_integer(),    % maximum number of elements
    mb    :: non_neg_integer(),    % 2^mb = m, the size of each slice (bitvector)
    size  :: non_neg_integer(),    % number of elements
    a     :: [bitmask()]            % list of bitvectors
}).

-record(sbf, {
    e     :: float(),              % error probability
    r     :: float(),              % error probability ratio
    s     :: non_neg_integer(),    % log 2 of growth ratio
    size  :: non_neg_integer(),    % number of elements
    b     :: [#bloom{}]            % list of plain bloom filters
}).

%% Constructors for (fixed capacity) bloom filters
%%
%% N - capacity
%% E - error probability
bloom(N) -> bloom(N, 0.001).
bloom(N, E) when is_number(N), N > 0,
                 is_float(E), E > 0, E < 1,
                 N >= 4/E -> % rule of thumb; due to double hashing
    bloom(size, N, E);
bloom(N, E) when is_number(N), N >= 0,
                 is_float(E), E > 0, E < 1 ->
    bloom(bits, 32, E).

bloom(Mode, N, E) ->
    K = case Mode of
            size -> 1 + trunc(log2(1/E));
            bits -> 1
        end,
    P = pow(E, 1 / K),

    Mb =
        case Mode of
            size ->
                1 + trunc(-log2(1 - pow(1 - P, 1 / N)));
            bits ->
                N
        end,
    M = 1 bsl Mb,
    D = trunc(log(1-P) / log(1-1/M)),
    #bloom{e=E, n=D, mb=Mb, size = 0,
           a = [bitmask_new(Mb) || _ <- lists:seq(1, K)]}.

log2(X) -> log(X) / log(2).

%% Constructors for scalable bloom filters
%%
%% N - initial capacity before expanding
%% E - error probability
%% S - growth ratio when full (log 2) can be 1, 2 or 3
%% R - tightening ratio of error probability
sbf(N) -> sbf(N, 0.001).
sbf(N, E) -> sbf(N, E, 1).
sbf(N, E, 1) -> sbf(N, E, 1, 0.85);
sbf(N, E, 2) -> sbf(N, E, 2, 0.75);
sbf(N, E, 3) -> sbf(N, E, 3, 0.65).
sbf(N, E, S, R) when is_number(N), N > 0,
                     is_float(E), E > 0, E < 1,
                     is_integer(S), S > 0, S < 4,
                     is_float(R), R > 0, R < 1,
                     N >= 4/(E*(1-R)) -> % rule of thumb; due to double hashing
  #sbf{e=E, s=S, r=R, size=0, b=[bloom(N, E*(1-R))]}.

%% Returns number of elements
%%
size(#bloom{size=Size}) -> Size;
size(#sbf{size=Size}) -> Size.

%% Returns capacity
%%
capacity(#bloom{n=N}) -> N;
capacity(#sbf{}) -> infinity.

%% Test for membership
%%
member(Elem, #bloom{mb=Mb}=B) ->
    Hashes = make_hashes(Mb, Elem),
    hash_member(Hashes, B);
member(Elem, #sbf{b=[H|_]}=Sbf) ->
    Hashes = make_hashes(H#bloom.mb, Elem),
    hash_member(Hashes, Sbf).

hash_member(Hashes, #bloom{mb=Mb, a=A}) ->
    Mask = 1 bsl Mb -1,
    {I1, I0} = make_indexes(Mask, Hashes),
    all_set(Mask, I1, I0, A);
hash_member(Hashes, #sbf{b=B}) ->
    lists:any(fun(X) -> hash_member(Hashes, X) end, B).

make_hashes(Mb, E) when Mb =< 16 ->
    erlang:phash2({E}, 1 bsl 32);
make_hashes(Mb, E) when Mb =< 32 ->
    {erlang:phash2({E}, 1 bsl 32), erlang:phash2([E], 1 bsl 32)}.

make_indexes(Mask, {H0, H1}) when Mask > 1 bsl 16 -> masked_pair(Mask, H0, H1);
make_indexes(Mask, {H0, _}) -> make_indexes(Mask, H0);
make_indexes(Mask, H0) -> masked_pair(Mask, H0 bsr 16, H0).

masked_pair(Mask, X, Y) -> {X band Mask, Y band Mask}.

all_set(_Mask, _I1, _I, []) -> true;
all_set(Mask, I1, I, [H|T]) ->
    bitmask_get(I, H) andalso all_set(Mask, I1, (I+I1) band Mask, T).

%% Adds element to set
%%
add(Elem, #bloom{mb=Mb} = B) ->
    Hashes = make_hashes(Mb, Elem),
    hash_add(Hashes, B);
add(Elem, #sbf{size=Size, r=R, s=S, b=[H|T]=Bs}=Sbf) ->
    #bloom{mb=Mb, e=E, n=N, size=HSize} = H,
    Hashes = make_hashes(Mb, Elem),
    case hash_member(Hashes, Sbf) of
        true -> Sbf;
        false ->
            case HSize < N of
                true -> Sbf#sbf{size=Size+1, b=[hash_add(Hashes, H)|T]};
                false ->
                    B = add(Elem, bloom(bits, Mb + S, E * R)),
                    Sbf#sbf{size=Size+1, b=[B|Bs]}
            end
    end.

hash_add(Hashes, #bloom{mb=Mb, a=A, size=Size} = B) ->
    Mask = 1 bsl Mb -1,
    {I1, I0} = make_indexes(Mask, Hashes),
    B#bloom{size=Size+1, a=set_bits(Mask, I1, I0, A, [])}.

set_bits(_Mask, _I1, _I, [], Acc) -> lists:reverse(Acc);
set_bits(Mask, I1, I, [H|T], Acc) ->
    set_bits(Mask, I1, (I+I1) band Mask, T, [bitmask_set(I, H) | Acc]).


%%%========== Dispatch to appropriate representation:
bitmask_new(LogN) ->
    if LogN >= 20 -> % Use sparse representation.
            hanoidb_sparse_bitmap:new(LogN);
       true ->       % Use dense representation.
            hanoidb_dense_bitmap:new(1 bsl LogN)
    end.

bitmask_set(I, BM) ->
    case element(1,BM) of
        array -> bitarray_set(I, as_array(BM));
        sparse_bitmap -> hanoidb_sparse_bitmap:set(I, BM);
        dense_bitmap_ets -> hanoidb_dense_bitmap:set(I, BM);
        dense_bitmap ->
            %% Surprise - we need to mutate a built representation:
            hanoidb_dense_bitmap:set(I, hanoidb_dense_bitmap:unbuild(BM))
    end.

%%% Convert to external form.
bitmask_build(BM) ->
    case element(1,BM) of
        array -> BM;
        sparse_bitmap -> BM;
        dense_bitmap_ets -> hanoidb_dense_bitmap:build(BM)
    end.

bitmask_get(I, BM) ->
    case element(1,BM) of
        array -> bitarray_get(I, as_array(BM));
        sparse_bitmap -> hanoidb_sparse_bitmap:member(I, BM);
        dense_bitmap_ets -> hanoidb_dense_bitmap:member(I, BM);
        dense_bitmap     -> hanoidb_dense_bitmap:member(I, BM)
    end.

-spec as_array(bitmask()) -> array().
as_array(BM) ->
    case array:is_array(BM) of
        true -> BM
    end.

%%%========== Bitarray representation - suitable for sparse arrays ==========
bitarray_new(N) -> array:new((N-1) div ?W + 1, {default, 0}).

-spec bitarray_set( non_neg_integer(), array() ) -> array().
bitarray_set(I, A1) ->
    A = as_array(A1),
    AI = I div ?W,
    V = array:get(AI, A),
    V1 = V bor (1 bsl (I rem ?W)),
    if V =:= V1 -> A; % The bit is already set
       true -> array:set(AI, V1, A)
    end.

-spec bitarray_get( non_neg_integer(), array() ) -> boolean().
bitarray_get(I, A) ->
    AI = I div ?W,
    V = array:get(AI, A),
    (V band (1 bsl (I rem ?W))) =/= 0.

%%%^^^^^^^^^^ Bitarray representation - suitable for sparse arrays ^^^^^^^^^^

encode(Bloom) ->
    zlib:gzip(term_to_binary(bloom_build(Bloom))).

decode(Bin) ->
    binary_to_term(zlib:gunzip(Bin)).

%%% Convert to external form.
bloom_build(Bloom=#bloom{a=Bitmasks}) ->
    Bloom#bloom{a=[bitmask_build(X) || X <- Bitmasks]};
bloom_build(Sbf=#sbf{b=Blooms}) ->
    Sbf#sbf{b=[bloom_build(X) || X <- Blooms]}.

%% UNIT TESTS

-ifdef(TEST).
-ifdef(EQC).

prop_bloom_test_() ->
    {timeout, 60, fun() -> ?assert(eqc:quickcheck(prop_bloom())) end}.

g_keys() ->
    non_empty(list(non_empty(binary()))).

prop_bloom() ->
    ?FORALL(Keys, g_keys(),
            begin
                Bloom = ?MODULE:bloom(Keys),
                F = fun(X) -> member(X, Bloom) end,
                lists:all(F, Keys)
            end).

-endif.
-endif.
