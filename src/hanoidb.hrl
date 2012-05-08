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


%% smallest levels are 256 entries
-define(TOP_LEVEL, 8).
-define(BTREE_SIZE(Level), (1 bsl (Level))).
-define(FIRST_BLOCK_POS, 4).

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
