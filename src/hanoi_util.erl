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

encode_index_node(Level, KVList, Compress) ->

    TermData = erlang:term_to_binary(KVList),

    case Compress of
        snappy ->
            {ok, Snappied} = snappy:compress(TermData),
            CompressedData = [?SNAPPY_COMPRESSION|Snappied];
        gzip ->
            CompressedData = [?GZIP_COMPRESSION|zlib:gzip(TermData)];
        _ ->
            CompressedData = [?NO_COMPRESSION|TermData]
    end,

    Size = erlang:iolist_size(CompressedData),

    {ok, Size+6, [ <<(Size+2):32/unsigned, Level:16/unsigned>> | CompressedData ] }.

decode_index_node(Level, <<Tag, Data/binary>>) ->

    case Tag of
        ?NO_COMPRESSION ->
            TermData = Data;
        ?SNAPPY_COMPRESSION ->
            {ok, TermData} = snappy:decompress(Data);
        ?GZIP_COMPRESSION ->
            TermData = zlib:gunzip(Data)
    end,

    KVList = erlang:binary_to_term(TermData),
    {ok, {node, Level, KVList}}.


file_exists(FileName) ->
    case file:read_file_info(FileName) of
        {ok, _} ->
            true;
        {error, enoent} ->
            false
    end.
