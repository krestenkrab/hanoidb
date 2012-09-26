%% ----------------------------------------------------------------------------
%%
%% hanoidb: LSM-trees (Log-Structured Merge Trees) Indexed Storage
%%
%% Copyright 2011-2012 (c) Trifork A/S.  All Rights Reserved.
%% http://trifork.com/ info@trifork.com
%%
%% Copyright 2012 (c) Basho Technologies, Inc.  All Rights Reserved.
%% http://basho.com/ info@basho.com
%%
%% This file is provided to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
%% License for the specific language governing permissions and limitations
%% under the License.
%%
%% ----------------------------------------------------------------------------

-module(hanoidb_util).
-author('Kresten Krab Thorup <krab@trifork.com>').

-export([  compress/2
         , uncompress/1
         , index_file_name/1
         , estimate_node_size_increment/3
         , encode_index_node/2
         , decode_index_node/2
         , crc_encapsulate_kv_entry/2
         , decode_crc_data/3
         , file_exists/1
         , crc_encapsulate_transaction/2
         , tstamp/0
         , expiry_time/1
         , has_expired/1
         , ensure_expiry/1 ]).

-include("src/hanoidb.hrl").

-define(ERLANG_ENCODED,  131).
-define(CRC_ENCODED,     127).

-define(TAG_KV_DATA,  16#80).
-define(TAG_DELETED,  16#81).
-define(TAG_POSLEN32, 16#82).
-define(TAG_TRANSACT, 16#83).
-define(TAG_KV_DATA2, 16#84).
-define(TAG_DELETED2, 16#85).
-define(TAG_END,      16#FF).

-compile({inline, [crc_encapsulate/1, crc_encapsulate_kv_entry/2 ]}).


-spec index_file_name(string()) -> string().
index_file_name(Name) ->
    Name.

-spec file_exists(string()) -> boolean().
file_exists(FileName) ->
    case file:read_file_info(FileName) of
        {ok, _} ->
            true;
        {error, enoent} ->
            false
    end.

estimate_node_size_increment(_KVList, Key, {Value, _TStamp})
  when is_integer(Value) -> byte_size(Key) + 5 + 4;
estimate_node_size_increment(_KVList, Key, {Value, _TStamp})
  when is_binary(Value) -> byte_size(Key) + 5 + 4 + byte_size(Value);
estimate_node_size_increment(_KVList, Key, {Value, _TStamp})
  when is_atom(Value) -> byte_size(Key) + 8 + 4;
estimate_node_size_increment(_KVList, Key, {Value, _TStamp})
  when is_tuple(Value) -> byte_size(Key) + 13 + 4;
estimate_node_size_increment(_KVList, Key, Value)
  when is_integer(Value) -> byte_size(Key) + 5 + 4;
estimate_node_size_increment(_KVList, Key, Value)
  when is_binary(Value) -> byte_size(Key) + 5 + 4 + byte_size(Value);
estimate_node_size_increment(_KVList, Key, Value)
  when is_atom(Value) -> byte_size(Key) + 8 + 4;
estimate_node_size_increment(_KVList, Key, Value)
  when is_tuple(Value) -> byte_size(Key) + 13 + 4.

-define(NO_COMPRESSION, 0).
-define(SNAPPY_COMPRESSION, 1).
-define(GZIP_COMPRESSION, 2).
-define(LZ4_COMPRESSION, 3).

use_compressed(UncompressedSize, CompressedSize) when CompressedSize < UncompressedSize ->
    true;
use_compressed(_UncompressedSize, _CompressedSize) ->
    false.

compress(snappy, Bin) ->
    {ok, CompressedBin} = snappy:compress(Bin),
    case use_compressed(erlang:iolist_size(Bin), erlang:iolist_size(CompressedBin)) of
        true ->
            {?SNAPPY_COMPRESSION, CompressedBin};
        false ->
            {?NO_COMPRESSION, Bin}
    end;
compress(lz4, Bin) ->
    {ok, CompressedBin} = lz4:compress(erlang:iolist_to_binary(Bin)),
    case use_compressed(erlang:iolist_size(Bin), erlang:iolist_size(CompressedBin)) of
        true ->
            {?LZ4_COMPRESSION, CompressedBin};
        false ->
            {?NO_COMPRESSION, Bin}
    end;
compress(gzip, Bin) ->
    CompressedBin = zlib:gzip(Bin),
    case use_compressed(erlang:iolist_size(Bin), erlang:iolist_size(CompressedBin)) of
        true ->
            {?GZIP_COMPRESSION, CompressedBin};
        false ->
            {?NO_COMPRESSION, Bin}
    end;
compress(none, Bin) ->
    {?NO_COMPRESSION, Bin}.

uncompress(<<?NO_COMPRESSION, Data/binary>>) ->
    Data;
uncompress(<<?SNAPPY_COMPRESSION, Data/binary>>) ->
    {ok, UncompressedData} = snappy:decompress(Data),
    UncompressedData;
uncompress(<<?LZ4_COMPRESSION, Data/binary>>) ->
    lz4:uncompress(Data);
uncompress(<<?GZIP_COMPRESSION, Data/binary>>) ->
    zlib:gunzip(Data).

encode_index_node(KVList, Method) ->
    TermData = [ ?TAG_END |
                 lists:map(fun ({Key,Value}) ->
                                   crc_encapsulate_kv_entry(Key, Value)
                           end,
                           KVList) ],
    {MethodName, OutData} = compress(Method, TermData),
    {ok, [MethodName | OutData]}.

decode_index_node(Level, Data) ->
    TermData = uncompress(Data),
    {ok, KVList} = decode_kv_list(TermData),
    {ok, {node, Level, KVList}}.


-spec crc_encapsulate_kv_entry(binary(), expvalue()) -> iolist().
crc_encapsulate_kv_entry(Key, {Value, infinity}) ->
    crc_encapsulate_kv_entry(Key, Value);
crc_encapsulate_kv_entry(Key, {?TOMBSTONE, TStamp}) -> %
    crc_encapsulate( [?TAG_DELETED2, <<TStamp:32>> | Key] );
crc_encapsulate_kv_entry(Key, ?TOMBSTONE) ->
    crc_encapsulate( [?TAG_DELETED | Key] );
crc_encapsulate_kv_entry(Key, {Value, TStamp}) when is_binary(Value) ->
    crc_encapsulate( [?TAG_KV_DATA2, <<TStamp:32, (byte_size(Key)):32/unsigned>>, Key, Value] );
crc_encapsulate_kv_entry(Key, Value) when is_binary(Value) ->
    crc_encapsulate( [?TAG_KV_DATA, <<(byte_size(Key)):32/unsigned>>, Key, Value] );
crc_encapsulate_kv_entry(Key, {Pos,Len}) when Len < 16#ffffffff ->
    crc_encapsulate( [?TAG_POSLEN32, <<Pos:64/unsigned, Len:32/unsigned>>, Key] ).

-spec crc_encapsulate_transaction( [ txspec() ], expiry() ) -> iolist().
crc_encapsulate_transaction(TransactionSpec, Expiry) ->
    crc_encapsulate([?TAG_TRANSACT |
             lists:map(fun({delete, Key}) ->
                                crc_encapsulate_kv_entry(Key, {?TOMBSTONE, Expiry});
                           ({put, Key, Value}) ->
                                crc_encapsulate_kv_entry(Key, {Value, Expiry})
                        end,
                        TransactionSpec)]).

-spec crc_encapsulate( iolist() ) -> iolist().
crc_encapsulate(Blob) ->
    CRC = erlang:crc32(Blob),
    Size = erlang:iolist_size(Blob),
    [<< (Size):32/unsigned, CRC:32/unsigned >>, Blob, ?TAG_END].

-spec decode_kv_list( binary() ) -> {ok, [ kventry() ]}  | {partial, [kventry()], iolist()}.
decode_kv_list(<<?TAG_END, Custom/binary>>) ->
    decode_crc_data(Custom, [], []);
decode_kv_list(<<?ERLANG_ENCODED, _/binary>>=TermData) ->
    {ok, erlang:term_to_binary(TermData)};
decode_kv_list(<<?CRC_ENCODED, Custom/binary>>) ->
    decode_crc_data(Custom, [], []).

-spec decode_crc_data(binary(), list(), list()) -> {ok, [kventry()]} | {partial, [kventry()], iolist()}.
decode_crc_data(<<>>, [], Acc) ->
    {ok, lists:reverse(Acc)};
decode_crc_data(<<>>, BrokenData, Acc) ->
    {partial, lists:reverse(Acc), BrokenData};
    % TODO: we *could* simply return the good parts of the data...
    % would that be so wrong?
decode_crc_data(<< BinSize:32/unsigned, CRC:32/unsigned, Bin:BinSize/binary, ?TAG_END, Rest/binary >>, Broken, Acc) ->
    CRCTest = erlang:crc32( Bin ),
    if CRC == CRCTest ->
            decode_crc_data(Rest, Broken, [decode_kv_data(Bin) | Acc]);
       true ->
            % TODO: chunk is broken, ignore it. Maybe we should tell someone?
            decode_crc_data(Rest, [Bin|Broken], Acc)
    end;
decode_crc_data(Bad, Broken, Acc) ->
    %% If a chunk is broken, try to find the next ?TAG_END and
    %% start decoding from there.
    {Skipped, MaybeGood} = find_next_value(Bad),
    decode_crc_data(MaybeGood, [Skipped|Broken], Acc).

-spec find_next_value(binary()) -> { binary(), binary() }.
find_next_value(<<>>) ->
    {<<>>, <<>>};
find_next_value(Bin) ->
    case binary:match (Bin, <<?TAG_END>>) of
        {Pos, _Len} ->
            <<SkipBin :Pos /binary, ?TAG_END, MaybeGood /binary>> = Bin,
            {SkipBin, MaybeGood};
        nomatch ->
            {Bin, <<>>}
    end.

-spec decode_kv_data( binary() ) -> kventry().
decode_kv_data(<<?TAG_KV_DATA, KLen:32/unsigned, Key:KLen/binary, Value/binary >>) ->
    {Key, Value};
decode_kv_data(<<?TAG_DELETED, Key/binary>>) ->
    {Key, ?TOMBSTONE};
decode_kv_data(<<?TAG_KV_DATA2, TStamp:32/unsigned, KLen:32/unsigned, Key:KLen/binary, Value/binary >>) ->
    {Key, {Value, TStamp}};
decode_kv_data(<<?TAG_DELETED2, TStamp:32/unsigned, Key/binary>>) ->
    {Key, {?TOMBSTONE, TStamp}};
decode_kv_data(<<?TAG_POSLEN32, Pos:64/unsigned, Len:32/unsigned, Key/binary>>) ->
    {Key, {Pos,Len}};
decode_kv_data(<<?TAG_TRANSACT, Rest/binary>>) ->
    {ok, TX} = decode_crc_data(Rest, [], []),
    TX.

%% @doc Return number of seconds since 1970
-spec tstamp() -> pos_integer().
tstamp() ->
    {Mega, Sec, _Micro} = os:timestamp(),
    (Mega * 1000000) + Sec.

%% @doc Return time when values expire (i.e. Now + ExpirySecs), or 0.
-spec expiry_time(pos_integer()) -> pos_integer().
expiry_time(ExpirySecs) when ExpirySecs > 0 ->
    tstamp() + ExpirySecs.

-spec has_expired(pos_integer()) -> true|false.
has_expired(Expiration) when Expiration > 0 ->
    Expiration < tstamp();
has_expired(infinity) ->
    false.


ensure_expiry(Opts) ->
    case hanoidb:get_opt(expiry_secs, Opts) of
        undefined ->
            try exit(err)
            catch
                exit:err ->
                    io:format(user, "~p~n", [erlang:get_stacktrace()])
            end,
            exit(expiry_secs_not_set);
        N when N >= 0 ->
            ok
    end.


