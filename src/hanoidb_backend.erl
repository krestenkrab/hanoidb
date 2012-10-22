
-module(hanoidb_backend).

-include("include/hanoidb.hrl").
-include("src/hanoidb.hrl").

-type options() :: [ atom() | { atom(), term() } ].
-type kvexp_entry() :: { Key :: key(), Value :: value(), TimeOut :: expiry() }.
-type batch_reader() :: any().
-type batch_writer() :: any().
-type random_reader() :: any().

-export([merge/7]).

%%%=========================================================================
%%%  API
%%%=========================================================================

%% batch_reader and batch_writer are used by the merging logic.  A batch_reader
%% must return the values in lexicographical order of the binary keys.

-callback open_batch_reader(File :: string(), Options :: options())
    -> {ok, batch_reader()} | { error, term() }.
-callback read_next(batch_reader())
    -> { [kvexp_entry(), ...], batch_reader()} | 'done'.
-callback close_batch_reader( batch_reader() )
    -> ok | {error, term()}.


-callback open_batch_writer(File :: string(), Options :: options())
    -> {ok, batch_writer()} | {error, term()}.
-callback write_next( kvexp_entry() , batch_writer() )
    -> {ok, batch_writer()} | {error, term()}.
-callback write_count( batch_writer() ) ->
    {ok, non_neg_integer()} | {error, term()}.
-callback close_batch_writer( batch_writer() )
    -> ok | {error, term()}.


-callback open_random_reader(File :: string(), Options :: options()) ->
    {ok, random_reader()} | {error, term()}.
-callback file_name( random_reader() ) ->
    {ok, string()} | {error, term()}.
-callback lookup( Key :: key(), random_reader() ) ->
    not_found | {ok, value()}.
-callback range_fold( fun( (key(), value(), term()) -> term() ),
                      Acc0   :: term(),
                      Reader :: random_reader(),
                      Range  :: #key_range{} ) ->
    {limit, term(), LastKey :: binary()} | {ok, term()}.
-callback close_random_reader(random_reader()) ->
    ok | {error, term()}.




-spec merge(atom(), string(), string(), string(), integer(), boolean(), list()) -> {ok, integer()}.
merge(Mod,A,B,C, Size, IsLastLevel, Options) ->
    {ok, IXA} = Mod:open_batch_reader(A, Options),
    {ok, IXB} = Mod:open_batch_reader(B, Options),
    {ok, Out} = Mod:open_batch_writer(C, [{size, Size} | Options]),
    scan(Mod,IXA, IXB, Out, IsLastLevel, [], [], {0, none}).

terminate(Mod, Out) ->
    {ok, Count} = Mod:write_count( Out ),
    ok = Mod:close_batch_writer( Out ),
    {ok, Count}.

step(S) ->
    step(S, 1).

step({N, From}, Steps) ->
    {N-Steps, From}.


scan(Mod, IXA, IXB, Out, IsLastLevel, AKVs, BKVs, {N, FromPID}) when N < 1, AKVs =/= [], BKVs =/= [] ->
    case FromPID of
        none ->
            ok;
        {PID, Ref} ->
            PID ! {Ref, step_done}
    end,

    receive
        {step, From, HowMany} ->
            scan(Mod, IXA, IXB, Out, IsLastLevel, AKVs, BKVs, {N+HowMany, From})
    end;

scan(Mod, IXA, IXB, Out, IsLastLevel, [], BKVs, Step) ->
    case Mod:read_next(IXA) of
        {AKVs, IXA2} ->
            scan(Mod, IXA2, IXB, Out, IsLastLevel, AKVs, BKVs, Step);
        done ->
            ok = Mod:close_batch_reader(IXA),
            scan_only(Mod, IXB, Out, IsLastLevel, BKVs, Step)
    end;

scan(Mod, IXA, IXB, Out, IsLastLevel, AKVs, [], Step) ->
    case Mod:read_next(IXB) of
        {BKVs, IXB2} ->
            scan(Mod, IXA, IXB2, Out, IsLastLevel, AKVs, BKVs, Step);
        done ->
            ok = Mod:close_batch_reader(IXB),
            scan_only(Mod, IXA, Out, IsLastLevel, AKVs, Step)
    end;

scan(Mod, IXA, IXB, Out, IsLastLevel, [{Key1,_,_}=Entry|AT], [{Key2,_,_}|_]=BKVs, Step)
  when Key1 < Key2 ->
    case Entry of
        {_, ?TOMBSTONE, _} when IsLastLevel ->
            scan(Mod, IXA, IXB, Out, true, AT, BKVs, step(Step));
        _ ->
            {ok, Out3} = Mod:write_next( Entry, Out ),
            scan(Mod, IXA, IXB, Out3, IsLastLevel, AT, BKVs, step(Step))
    end;
scan(Mod, IXA, IXB, Out, IsLastLevel, [{Key1,_,_}|_]=AKVs, [{Key2,_,_}=Entry|BT], Step)
  when Key1 > Key2 ->
    case Entry of
        {_, ?TOMBSTONE, _} when IsLastLevel ->
            scan(Mod, IXA, IXB, Out, true, AKVs, BT, step(Step));
        _ ->
            {ok, Out3} = Mod:write_next( Entry, Out ),
            scan(Mod, IXA, IXB, Out3, IsLastLevel, AKVs, BT, step(Step))
    end;
scan(Mod, IXA, IXB, Out, IsLastLevel, [_|AT], [Entry|BT], Step) ->
    case Entry of
        {_, ?TOMBSTONE, _} when IsLastLevel ->
            scan(Mod, IXA, IXB, Out, true, AT, BT, step(Step));
        _ ->
            {ok, Out3} = Mod:write_next( Entry, Out ),
            scan(Mod, IXA, IXB, Out3, IsLastLevel, AT, BT, step(Step, 2))
    end.


scan_only(Mod, IX, Out, IsLastLevel, KVs, {N, FromPID}) when N < 1, KVs =/= [] ->
    case FromPID of
        none ->
            ok;
        {PID, Ref} ->
            PID ! {Ref, step_done}
    end,

    receive
        {step, From, HowMany} ->
            scan_only(Mod, IX, Out, IsLastLevel, KVs, {N+HowMany, From})
    end;

scan_only(Mod, IX, Out, IsLastLevel, [], {_, FromPID}=Step) ->
    case Mod:read_next(IX) of
        {KVs, IX2} ->
            scan_only(Mod, IX2, Out, IsLastLevel, KVs, Step);
        done ->
            case FromPID of
                none ->
                    ok;
                {PID, Ref} ->
                    PID ! {Ref, step_done}
            end,
            ok = Mod:close_batch_reader(IX),
            terminate(Mod, Out)
    end;

scan_only(Mod, IX, Out, true, [{_,?TOMBSTONE,_}|Rest], Step) ->
    scan_only(Mod, IX, Out, true, Rest, step(Step));

scan_only(Mod, IX, Out, IsLastLevel, [Entry|Rest], Step) ->
    {ok, Out3} = Mod:write_next( Entry, Out ),
    scan_only(Mod, IX, Out3, IsLastLevel, Rest, step(Step)).






