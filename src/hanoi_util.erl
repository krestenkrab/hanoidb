%% ----------------------------------------------------------------------------
%%
%% hanoi: LSM-trees (Log-Structured Merge Trees) Indexed Storage
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

-module(hanoi_util).
-author('Kresten Krab Thorup <krab@trifork.com>').

-compile(export_all).

-include("src/hanoi.hrl").

index_file_name(Name) ->
    Name.

estimate_node_size_increment(_KVList,Key,Value) ->
    byte_size(Key)
        + 10
        + if
              is_integer(Value) ->
                  5;
              is_binary(Value) ->
                  5 + byte_size(Value);
              is_atom(Value) ->
                  8;
              is_tuple(Value) ->
                  13
          end.

-define(NO_COMPRESSION, 0).
-define(SNAPPY_COMPRESSION, 1).
-define(GZIP_COMPRESSION, 2).

encode_index_node(KVList, Compress) ->

    TermData = encode_kv_list(KVList),

    case Compress of
        snappy ->
            DataSize = erlang:iolist_size(TermData),
            {ok, Snappied} = snappy:compress(TermData),
            if byte_size(Snappied) > DataSize ->
                    OutData = [?NO_COMPRESSION|TermData];
               true ->
                    OutData = [?SNAPPY_COMPRESSION|Snappied]
            end;
        gzip ->
            DataSize = erlang:iolist_size(TermData),
            GZipData = zlib:gzip(TermData),
            if byte_size(GZipData) > DataSize ->
                    OutData = [?NO_COMPRESSION|TermData];
               true ->
                    OutData = [?GZIP_COMPRESSION|GZipData]
            end;
        _ ->
            OutData = [?NO_COMPRESSION|TermData]
    end,

    {ok, OutData}.

decode_index_node(Level, <<Tag, Data/binary>>) ->

    case Tag of
        ?NO_COMPRESSION ->
            TermData = Data;
        ?SNAPPY_COMPRESSION ->
            {ok, TermData} = snappy:decompress(Data);
        ?GZIP_COMPRESSION ->
            TermData = zlib:gunzip(Data)
    end,

    KVList = decode_kv_list(TermData),
    {ok, {node, Level, KVList}}.


file_exists(FileName) ->
    case file:read_file_info(FileName) of
        {ok, _} ->
            true;
        {error, enoent} ->
            false
    end.


-define(ERLANG_ENCODED,  131).
-define(CRC_ENCODED,     127).

-define(TAG_KV_DATA,  16#80).
-define(TAG_DELETED,  16#81).
-define(TAG_POSLEN32, 16#82).
-define(TAG_END,      16#FF).

encode(Blob) ->
    CRC = erlang:crc32(Blob),
    Size = erlang:iolist_size(Blob),
    [ << (Size):32/unsigned, CRC:32/unsigned >>, Blob, ?TAG_END ].

encode_kv_list(KVList) ->
    [ ?CRC_ENCODED |
      lists:foldl(fun({Key,Value}, Acc) when is_binary(Key), is_binary(Value) ->
                          [ encode( [?TAG_KV_DATA, <<(byte_size(Key)):32/unsigned>>, Key, Value] ) | Acc ];
                     ({Key, ?TOMBSTONE}, Acc) ->
                          [ encode( [?TAG_DELETED, Key] ) | Acc ];
                     ({Key, {Pos,Len}}, Acc) when Len < 16#ffffffff ->
                          [ encode( [?TAG_POSLEN32, <<Pos:64/unsigned, Len:32/unsigned>>, Key ] ) | Acc ]
                  end,
                  [],
                  KVList) ].


decode_kv_list(<<?ERLANG_ENCODED, _/binary>>=TermData) ->
    erlang:term_to_binary(TermData);

decode_kv_list(<<?CRC_ENCODED, Custom/binary>>) ->
    decode_crc_data(Custom, []).



decode_crc_data(<<>>, Acc) ->
    Acc;

decode_crc_data(<< BinSize:32/unsigned, CRC:32/unsigned, Bin:BinSize/binary, ?TAG_END, Rest/binary >>, Acc) ->
    CRCTest = erlang:crc32( Bin ),
    if CRC == CRCTest ->
            decode_crc_data(Rest, [ decode_kv_data( Bin ) | Acc ]);
       true ->
            %% chunk is broken, ignore it. Maybe we should tell someone?
            decode_crc_data(Rest, Acc)
    end;

decode_crc_data(Bad, Acc) ->
    %% if a chunk is broken, try to find the next ?TAG_END and
    %% start decoding from there.
    decode_crc_data(find_next_value(Bad), Acc).

find_next_value(<<>>) ->
    <<>>;

find_next_value(Bin) ->
    case binary:match (Bin, <<?TAG_END>>) of
        {Pos, _Len} ->
            <<_SkipBin :Pos /binary, ?TAG_END, MaybeGood /binary>> = Bin,

            %% TODO: tell someone? that we skipped _SkipBin.  If we store
            %% the data somewhere, maybe something can be recovered
            %% from it ...

            MaybeGood;
        nomatch ->
            <<>>
    end.

decode_kv_data(<<?TAG_KV_DATA, KLen:32/unsigned, Key:KLen/binary, Value/binary >>) ->
    {Key, Value};

decode_kv_data(<<?TAG_DELETED, Key/binary>>) ->
    {Key, ?TOMBSTONE};

decode_kv_data(<<?TAG_POSLEN32, Pos:64/unsigned, Len:32/unsigned, Key/binary>>) ->
    {Key, {Pos,Len}}.


