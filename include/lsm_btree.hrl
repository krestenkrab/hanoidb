

%%
%% When doing "async fold", it does "sync fold" in chunks
%% of this many K/V entries.
%%
-define(BTREE_ASYNC_CHUNK_SIZE, 100).

%%
%% The btree_range structure is a bit assymetric, here is why:
%%
%% from_key=<<>> is "less than" any other key, hence we don't need to
%% handle from_key=undefined to support an open-ended start of the
%% interval. For to_key, we cannot (statically) construct a key
%% which is > any possible key, hence we need to allow to_key=undefined
%% as a token of an interval that has no upper limit.
%%
-record(btree_range, { from_key = <<>>       :: binary(),
                       from_inclusive = true :: boolean(),
                       to_key                :: binary() | undefined,
                       to_inclusive = false  :: boolean(),
                       limit :: pos_integer() | undefined }).
