# Hanoi Key/Value Storage Engine

This Erlang-based storage engine implements a structure somewhat like LSM-trees (Log-Structured Merge Trees, see docs/10.1.1.44.2782.pdf).  The notes below describe how this storage engine work; I have not done extensive studies as how it differs from other storage mechanisms, but a brief brows through available online resources on LSM-trees indicates that this storage engine is quite different in several respects.

The storage engine can function as an alternative backend for Basho's Riak/KV.

Here's the bullet list:

- Very fast writes and deletes,
- Reasonably fast reads (N records are stored in log<sub>2</sub>(N) B-trees),
- Operations-friendly "append-only" storage (allows you to backup live system, and crash-recovery is very simple)
- The cost of evicting stale key/values is amortized into insertion, so you don't need to schedule merge to happen at off-peak hours.
- Supports range queries (and thus eventually Riak 2i.)
- Doesn't need a boat load of RAM
- All in 1000 lines of pure Erlang code

Once we're a bit more stable, we'll provide a Riak backend.

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
