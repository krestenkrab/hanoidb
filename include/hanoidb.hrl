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


%%
%% When doing "async fold", it does "sync fold" in chunks
%% of this many K/V entries.
%%
-define(BTREE_ASYNC_CHUNK_SIZE, 100).

%%
%% The key_range structure is a bit assymetric, here is why:
%%
%% from_key=<<>> is "less than" any other key, hence we don't need to
%% handle from_key=undefined to support an open-ended start of the
%% interval. For to_key, we cannot (statically) construct a key
%% which is > any possible key, hence we need to allow to_key=undefined
%% as a token of an interval that has no upper limit.
%%
-record(key_range, {   from_key = <<>>       :: binary(),
                       from_inclusive = true :: boolean(),
                       to_key                :: binary() | undefined,
                       to_inclusive = false  :: boolean(),
                       limit                 :: pos_integer() | undefined }).
