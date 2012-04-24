# Hanoi Key/Value Storage Engine

This Erlang-based storage engine implements a structure somewhat like LSM-trees (Log-Structured Merge Trees, see docs/10.1.1.44.2782.pdf).  The notes below describe how this storage engine work; I have not done extensive studies as how it differs from other storage mechanisms, but a brief brows through available online resources on LSM-trees indicates that this storage engine is quite different in several respects.

The storage engine can function as an alternative backend for Basho's Riak/KV.

Here's the bullet list:

- Insert, Delete and Read all have worst case log<sub>2</sub>(N) complexity. 
- The cost of evicting stale key/values is amortized into insertion, so you don't need to schedule merge to happen at off-peak hours.
- Operations-friendly "append-only" storage (allows you to backup live system, and crash-recovery is very fast)
- Supports range queries (and thus eventually Riak 2i.)
- Doesn't need much RAM, but does need a lot of file descriptors
- All around 3000 lines of pure Erlang code

### Deploying the hanoi for testing with Riak/KV

You can deploy `hanoi` into a Riak devrel cluster using the
`enable-hanoi` script. Clone the `riak` repo, change your working directory
to it, and then execute the `enable-hanoi` script. It adds `hanoi` as a
dependency, runs `make all devrel`, and then modifies the configuration
settings of the resulting dev nodes to use the hanoi storage backend.

1. `git clone git://github.com/basho/riak.git`
1. `cd riak/deps`
1. `git clone git://github.com/basho/hanoi.git`
1. `cd ..`
1. `./deps/hanoi/enable-hanoi` # which does `make all devrel`

