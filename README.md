# HanoiDB Indexed Key/Value Storage

HanoiDB implements an indexed, key/value storage engine.  The primary index is
a log-structured merge tree (LSM-BTree) implemented using "doubling sizes"
persistent ordered sets of key/value pairs, similar is some regards to
[LevelDB](http://code.google.com/p/leveldb/).  HanoiDB includes a visualizer
which when used to watch a living database resembles the "Towers of Hanoi"
puzzle game, which inspired the name of this database.

## Features
- Insert, Delete and Read all have worst case *O*(log<sub>2</sub>(*N*)) latency.
- Incremental space reclaimation: The cost of evicting stale key/values
  is amortized into insertion
  - you don't need a separate eviction thread to keep memory use low
  - you don't need to schedule merges to happen at off-peak hours
- Operations-friendly "append-only" storage
  - allows you to backup live system
  - crash-recovery is very fast and the logic is straight forward
  - all data subject to CRC32 checksums
  - data can be compressed on disk to save space
- Efficient range queries
  - Riak secondary indexing
  - Fast key and bucket listing
- Uses bloom filters to avoid unnecessary lookups on disk
- Time-based expiry of data
  - configure the database to expire data older than n seconds
  - specify a lifetime in seconds for any particular key/value pair
- Efficient resource utilization
  - doesn't store all keys in memory
  - uses a modest number of file descriptors proportional to the number of levels
  - I/O is generally balanced between random and sequential
  - low CPU overhead
- ~2000 lines of pure Erlang code in src/*.erl

HanoiDB is developed by Trifork, a Riak expert solutions provider, and Basho
Technologies, makers of Riak.  HanoiDB can be used in Riak via the
`riak_kv_tower_backend` repository.

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


### Contributors

- Kresten Krab Thorup @krestenkrab
- Greg Burd @gburd
- Jesper Louis Andersen @jlouis
- Steve Vinoski @vinoski
- Erik Søe Sørensen, @eriksoe
- Yamamoto Takashi @yamt
- Joseph Wayne Norton @norton
