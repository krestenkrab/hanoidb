# Hanoi Ordered Key/Value Storage

Hanoi implements an ordered key/value storage engine, implemented
using "doubling sizes" persistent ordered sets of key/value pairs,
much like LevelDB.

Here's the bullet list:

- Insert, Delete and Read all have worst case log<sub>2</sub>(N) latency.
- Incremental space reclaimation: The cost of evicting stale key/values
  is amortized into insertion
  - you don't need a separate eviction thread to keep memory use low
  - you don't need to schedule merges to happen at off-peak hours
- Operations-friendly "append-only" storage
  - allows you to backup live system
  - crash-recovery is very fast and the logic is straight forward
- Supports efficient range queries
- Uses bloom filters to avoid unnecessary lookups on disk
- Efficient resource utilization
  - Doesn't store all keys in memory
  - Uses a modest number of file descriptors proportional to the number of levels
  - IO is generally balanced between random and sequential
  - Low CPU overhead
- ~2000 lines of pure Erlang code in src/*.erl

Hanoi is developed by Trifork, a Riak expert solutions provider.  You're most
welcome to contact us if you want help optimizing your Riak setup.

### How to deploy Hanoi as a Riak/KV backend

This storage engine can function as an alternative backend for Basho's Riak/KV.

You can deploy `hanoi` into a Riak devrel cluster using the `enable-hanoi`
script. Clone the `riak` repo, change your working directory to it, and then
execute the `enable-hanoi` script. It adds `hanoi` as a dependency, runs `make
all devrel`, and then modifies the configuration settings of the resulting dev
nodes to use the hanoi storage backend.

1. `git clone git://github.com/basho/riak.git`
1. `cd riak/deps`
1. `git clone git://github.com/basho/hanoi.git`
1. `cd ..`
1. `./deps/hanoi/enable-hanoi`
