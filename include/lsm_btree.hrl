

%%
%% When doing "async fold", it does "sync fold" in chunks
%% of this many K/V entries.
%%
-define(BTREE_ASYNC_CHUNK_SIZE, 100).

-record(btree_range, { from_key = <<>>       :: binary(),
                       from_inclusive = true :: boolean(),
                       to_key                :: binary() | undefined,
                       to_inclusive = false  :: boolean(),
                       limit :: pos_integer() | undefined }).
