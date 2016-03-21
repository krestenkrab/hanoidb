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


%% smallest levels are 1024 entries
-define(TOP_LEVEL, 10).
-define(BTREE_SIZE(Level), (1 bsl (Level))).
-define(FILE_FORMAT, <<"HAN2">>).
-define(FIRST_BLOCK_POS, byte_size(?FILE_FORMAT)).

-define(TOMBSTONE, 'deleted').

-define(KEY_IN_FROM_RANGE(Key,Range),
        ((Range#key_range.from_inclusive andalso
          (Range#key_range.from_key =< Key))
         orelse
           (Range#key_range.from_key < Key))).

-define(KEY_IN_TO_RANGE(Key,Range),
        ((Range#key_range.to_key == undefined)
         orelse
         ((Range#key_range.to_inclusive andalso
             (Key =< Range#key_range.to_key))
          orelse
             (Key <  Range#key_range.to_key)))).

-define(KEY_IN_RANGE(Key,Range),
        (?KEY_IN_FROM_RANGE(Key,Range) andalso ?KEY_IN_TO_RANGE(Key,Range))).


-ifdef(pre18).
-define(TIMESTAMP, now()).
-else.
-define(TIMESTAMP, erlang:timestamp()).
-endif.

-record(nursery, { log_file :: file:fd(),
                   dir :: string(),
                   cache :: gb_trees:tree(binary(), binary()),
                   total_size=0 :: integer(),
                   count=0 :: integer(),
                   last_sync=?TIMESTAMP :: erlang:timestamp(),
                   min_level :: integer(),
                   max_level :: integer(),
                   config=[] :: [{atom(), term()}],
                   step=0 :: integer(),
                   merge_done=0 :: integer()}).

-type kventry() :: { key(), expvalue() } | [ kventry() ].
-type key() :: binary().
-type txspec() :: { delete, key() } | { put, key(), value() }.
-type value() :: ?TOMBSTONE | binary().
-type expiry() :: infinity | integer().
-type filepos() :: { non_neg_integer(), non_neg_integer() }.
-type expvalue() :: { value(), expiry() }
                  | value()
                  | filepos().

-ifdef(USE_EBLOOM).
-define(HANOI_BLOOM_TYPE, ebloom).
-else.
-define(HANOI_BLOOM_TYPE, sbloom).
-endif.

-define(BLOOM_NEW(Size),            hanoidb_util:bloom_new(Size, ?HANOI_BLOOM_TYPE)).
-define(BLOOM_TO_BIN(Bloom),        hanoidb_util:bloom_to_bin(Bloom)).
-define(BIN_TO_BLOOM(Bin, Fmt),     hanoidb_util:bin_to_bloom(Bin, Fmt)).
-define(BLOOM_INSERT(Bloom, Key),   hanoidb_util:bloom_insert(Bloom, Key)).
-define(BLOOM_CONTAINS(Bloom, Key), hanoidb_util:bloom_contains(Bloom, Key)).

%% tags used in the on-disk representation
-define(TAG_KV_DATA,  16#80).
-define(TAG_DELETED,  16#81).
-define(TAG_POSLEN32, 16#82).
-define(TAG_TRANSACT, 16#83).
-define(TAG_KV_DATA2, 16#84).
-define(TAG_DELETED2, 16#85).
-define(TAG_END,      16#FF).


