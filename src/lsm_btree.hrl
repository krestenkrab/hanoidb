

%% smallest levels are 32 entries
-define(TOP_LEVEL, 5).
-define(BTREE_SIZE(Level), (1 bsl (Level))).

-define(TOMBSTONE, 'deleted').

-define(KEY_IN_RANGE(Key,FromKey,ToKey),
        (((FromKey == undefined) orelse (FromKey =< Key))
         and
         ((ToKey == undefined) orelse (Key < ToKey)))).
