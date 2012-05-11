# HanoiDB Ordered Key/Value Storage

HanoiDB implements an ordered key/value storage engine, implemented
using "doubling sizes" persistent ordered sets of key/value pairs,
much like LevelDB.

Here's the bullet list:

- Insert, Delete and Read all have worst case *O*(log<sub>2</sub>(*N*)) latency.
- Incremental space reclaimation: The cost of evicting stale key/values
  is amortized into insertion
  - you don't need a separate eviction thread to keep memory use low
  - you don't need to schedule merges to happen at off-peak hours
- Operations-friendly "append-only" storage
  - allows you to backup live system
  - crash-recovery is very fast and the logic is straight forward
  - All data subject to CRC32 checksums
- Supports efficient range queries
  - Riak secondary indexing
  - Fast key and bucket listing
- Uses bloom filters to avoid unnecessary lookups on disk
- Efficient resource utilization
  - Doesn't store all keys in memory
  - Uses a modest number of file descriptors proportional to the number of levels
  - IO is generally balanced between random and sequential
  - Low CPU overhead
- ~2000 lines of pure Erlang code in src/*.erl

HanoiDB is developed by Trifork, a Riak expert solutions provider.  You're most
welcome to contact us if you want help optimizing your Riak setup.

### Configuration options

Put these values in your `app.config` in the `hanoidb` section

```erlang
 {hanoidb, [
          {data_root, "./data/hanoidb"},

          %% Enable/disable on-disk compression.
          %%
          {compress, none | gzip},
          
          %% Expire (automatically delete) entries after N seconds.
          %% When this value is 0 (zero), entries never expire.
          %%
          {expiry_secs, 0},

          %% Sync strategy `none' only syncs every time the
          %% nursery runs full, which is currently hard coded
          %% to be evert 256 inserts or deletes.
          %%
          %% Sync strategy `sync' will sync the nursery log
          %% for every insert or delete operation.
          %%
          {sync_strategy, none | sync | {seconds, N}},

          %% The page size is a minimum page size, when a page fills
          %% up to beyond this size, it is written to disk.
          %% Compression applies to such units of page size.
          %%
          {page_size, 8192},

          %% Read/write buffer sizes apply to merge processes.
          %% A merge process has two read buffers and a write
          %% buffer, and there is a merge process *per level* in
          %% the database.
          %%
          {write_buffer_size, 524288},  % 512kB
          {read_buffer_size, 524288},  % 512kB

          %% The merge strategy is one of `fast' or `predictable'.
          %% Both have same log2(N) worst case, but `fast' is
          %% sometimes faster; yielding latency fluctuations.
          %%
          {merge_strategy, fast | predictable}
         ]},
```

### How to deploy HanoiDB as a Riak/KV backend

This storage engine can function as an alternative backend for Basho's Riak/KV.

You can deploy `hanoidb` into a Riak devrel cluster using the `enable-hanoidb`
script. Clone the `riak` repo, change your working directory to it, and then
execute the `enable-hanoidb` script. It adds `hanoidb` as a dependency, runs `make
all devrel`, and then modifies the configuration settings of the resulting dev
nodes to use the hanoidb storage backend.

1. `git clone git://github.com/basho/riak.git`
1. `mkdir riak/deps`
1. `cd riak/deps`
1. `git clone git://github.com/basho/hanoidb.git`
1. `cd ..`
1. `./deps/hanoidb/enable-hanoidb`
