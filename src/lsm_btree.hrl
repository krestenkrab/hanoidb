

%% smallest levels are 32 entries
-define(TOP_LEVEL, 5).
-define(BTREE_SIZE(Level), (1 bsl (Level))).

-define(TOMBSTONE, 'deleted').

-define(KEY_IN_FROM_RANGE(Key,Range),
        ((Range#btree_range.from_inclusive andalso
          (Range#btree_range.from_key =< Key))
         orelse
           (Range#btree_range.from_key < Key))).

-define(KEY_IN_TO_RANGE(Key,Range),
        ((Range#btree_range.to_key == undefined)
         orelse
         ((Range#btree_range.to_inclusive andalso
             (Key =< Range#btree_range.to_key))
          orelse
             (Key <  Range#btree_range.to_key)))).

-define(KEY_IN_RANGE(Key,Range),
        ((Range#btree_range.from_inclusive andalso
          (Range#btree_range.from_key =< Key))
         orelse
           (Range#btree_range.from_key < Key))
        andalso
        ((Range#btree_range.to_key == undefined)
         orelse
         ((Range#btree_range.to_inclusive andalso
             (Key =< Range#btree_range.to_key))
          orelse
             (Key <  Range#btree_range.to_key)))).


