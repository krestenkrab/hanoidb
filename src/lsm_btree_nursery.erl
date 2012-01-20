-module(lsm_btree_nursery).

-export([new/1, recover/2, add/3, finish/2, lookup/2, add_maybe_flush/4]).
-export([do_level_fold/4]).

-include("lsm_btree.hrl").
-include_lib("kernel/include/file.hrl").

-record(nursery, { log_file, dir, cache, total_size=0, count=0 }).

-spec new(string()) -> {ok, #nursery{}} | {error, term()}.

-define(LOGFILENAME(Dir), filename:join(Dir, "nursery.log")).

new(Directory) ->
    {ok, File} = file:open( ?LOGFILENAME(Directory),
                            [raw, exclusive, write, delayed_write, append]),
    {ok, #nursery{ log_file=File, dir=Directory, cache= gb_trees:empty() }}.


recover(Directory, TopLevel) ->
    case file:read_file_info( ?LOGFILENAME(Directory) ) of
        {ok, _} ->
            ok = do_recover(Directory, TopLevel),
            new(Directory);
        {error, enoent} ->
            new(Directory)
    end.

do_recover(Directory, TopLevel) ->
    %% repair the log file; storing it in nursery2
    LogFileName = ?LOGFILENAME(Directory),
    {ok, Nursery} = read_nursery_from_log(Directory),

    ok = finish(Nursery, TopLevel),

    %% assert log file is gone
    {error, enoent} = file:read_file_info(LogFileName),

    ok.

read_nursery_from_log(Directory) ->
    {ok, LogFile} = file:open( ?LOGFILENAME(Directory), [raw, read, read_ahead, binary] ),
    {ok, Cache} = load_good_chunks(LogFile, gb_trees:empty()),
    ok = file:close(LogFile),
    {ok, #nursery{ dir=Directory, cache=Cache, count=gb_trees:size(Cache) }}.

%% Just read the log file into a cache (gb_tree).
%% If any errors happen here, then we simply ignore them and return
%% the values we got so far.
load_good_chunks(File, Cache) ->
    case file:read(File, 8) of
        {ok, <<Length:32, BNotLength:32>>} when BNotLength =:= bnot Length->
            case file:read(File, Length) of
                {ok, EncData} when byte_size(EncData) == Length ->
                    try
                        <<KeySize:32/unsigned, Key:KeySize/binary, ValBin/binary>> = EncData,
                        case ValBin of
                            <<>> -> Value=?TOMBSTONE;
                            <<ValueSize:32/unsigned, Value/binary>> when ValueSize==byte_size(Value) -> ok
                        end,

                        %% TODO: is this tail recursive?  I don't think so
                        load_good_chunks(File, gb_trees:enter(Key, Value, Cache))
                    catch
                        _:_ -> {ok, Cache}
                    end;
                eof ->
                    {ok, Cache};
                {error, _} ->
                    {ok, Cache}
            end;
        _ ->
            {ok, Cache}
    end.


% @doc
% Add a Key/Value to the nursery
% @end
-spec add(#nursery{}, binary(), binary()|?TOMBSTONE) -> {ok, #nursery{}}.
add(Nursery=#nursery{ log_file=File, cache=Cache, total_size=TotalSize, count=Count }, Key, Value) ->

    Size = 4 + byte_size(Key)
        + if Value=:=?TOMBSTONE -> 0;
             true -> 4 + byte_size(Value) end,

    file:write(File, [<<Size:32/unsigned>>, <<(bnot Size):32/unsigned>>,
                      <<(byte_size(Key)):32>>, Key]
                     ++ if Value /= ?TOMBSTONE -> [<<(byte_size(Value)):32>>, Value];
                           true -> [] end),

    ok = file:datasync(File),

    Cache2 = gb_trees:enter(Key, Value, Cache),
    Nursery2 = Nursery#nursery{ cache=Cache2, total_size=TotalSize+Size+16, count=Count+1 },
    if
       Count+1 >= ?BTREE_SIZE(?TOP_LEVEL) ->
            {full, Nursery2};
       true ->
            {ok, Nursery2}
    end.

lookup(Key, #nursery{ cache=Cache }) ->
    gb_trees:lookup(Key, Cache).

% @doc
% Finish this nursery (encode it to a btree, and delete the nursery file)
% @end
-spec finish(Nursery::#nursery{}, TopLevel::pid()) -> ok.
finish(#nursery{ dir=Dir, cache=Cache, log_file=LogFile, total_size=_TotalSize, count=Count }, TopLevel) ->

    %% first, close the log file (if it is open)
    if LogFile /= undefined ->
            ok = file:close(LogFile);
       true ->
            ok
    end,

    case Count of
        N when N>0 ->
            %% next, flush cache to a new BTree
            BTreeFileName = filename:join(Dir, "nursery.data"),
            {ok, BT} = lsm_btree_writer:open(BTreeFileName, ?BTREE_SIZE(?TOP_LEVEL)),
            try
                lists:foreach( fun({Key,Value}) ->
                                       ok = lsm_btree_writer:add(BT, Key, Value)
                               end,
                               gb_trees:to_list(Cache))
            after
                ok = lsm_btree_writer:close(BT)
            end,

%            {ok, FileInfo} = file:read_file_info(BTreeFileName),
%            error_logger:info_msg("dumping log (count=~p, size=~p, outsize=~p)~n", 
%                                  [ gb_trees:size(Cache), TotalSize, FileInfo#file_info.size ]),

            %% inject the B-Tree (blocking RPC)
            ok = lsm_btree_level:inject(TopLevel, BTreeFileName);
        _ ->
            ok
    end,

    %% then, delete the log file
    LogFileName = filename:join(Dir, "nursery.log"),
    file:delete(LogFileName),
    ok.

add_maybe_flush(Key, Value, Nursery=#nursery{ dir=Dir }, Top) ->
    case add(Nursery, Key, Value) of
        {ok, _} = OK ->
            OK;
        {full, Nursery2} ->
            ok = lsm_btree_nursery:finish(Nursery2, Top),
            {error, enoent} = file:read_file_info( filename:join(Dir, "nursery.log")),
            lsm_btree_nursery:new(Dir)
    end.

do_level_fold(#nursery{ cache=Cache }, FoldWorkerPID, FromKey, ToKey) ->
    Ref = erlang:make_ref(),
    FoldWorkerPID ! {prefix, [Ref]},
    lists:foreach(fun({Key,Value}) when ?KEY_IN_RANGE(Key,FromKey,ToKey) ->
                          FoldWorkerPID ! {level_result, Ref, Key, Value};
                     (_) ->
                          ok
                  end,
                  gb_trees:to_list(Cache)),
    FoldWorkerPID ! {level_done, Ref},
    ok.
