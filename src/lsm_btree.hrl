

%% smallest levels are 32 entries
-define(TOP_LEVEL, 5).
-define(BTREE_SIZE(Level), (1 bsl (Level))).

-define(TOMBSTONE, 'deleted').

