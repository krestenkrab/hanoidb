# Fractal B-Tree Storage

This Erlang-based storage engine provides a scalable alternative to Basho Bitcask and Google's LevelDB with similar properties

- Very fast writes and deletes,
- Reasonably fast reads (N records are stored in log<sub>2</sub>(N) B-trees, each with a fan-out of 32),
- Operations-friendly "append-only" storage (allows you to backup live system)
- The cost of merging (evicting stale key/values) is amortized into insertion, so you don't need to schedule merge to happen at off-peak hours. 
- Supports range queries (and thus potentially Riak 2i.)
- Unlike Bitcask and InnoDB, you don't need a boat load of RAM
- All in 1000 lines of pure Erlang code

Once we're a bit more stable, we'll provide a Riak backend.

## How It Works

If there are N records, there are in log<sub>2</sub>(N)  levels (each an individual B-tree in a file).  Level #0 has 1 record, level #1 has 2 records, #2 has 4 records, and so on.  I.e. level #n has 2<sup>n</sup> records.

In "stable state", each level is either full or empty; so if there are e.g. 20 records stored, then levels #5 and #2 are full; the other ones are empty.

You can read more about Fractal Trees at [Tokutek](http://www.tokutek.com/2011/11/how-fractal-trees-work-at-mit-today/), a company providing a MySQL backend based on Fractal Trees.  I have not tried it, but it looks truly amazing.

### Lookup
Lookup is quite simple: starting at level #0, the sought for Key is searched in the B-tree there.  If nothing is found, search continues to the next level.  So if there are *N* levels, then *N* disk-based B-tree lookups are performed.  Each lookup is "guarded" by a bloom filter to improve the likelihood that disk-based searches are only done when likely to succeed.

### Insertion
Insertion works by a mechanism known as B-tree injection.  Insertion always starts by constructing a fresh B-tree with 1 element in it, and "injecting" that B-tree into level #0.  So you always inject a B-tree of the same size as the size of the level you're injecting it into.

- If the level being injected into empty, then the injected B-tree becomes the contents for that level. 
- Otherwise, the contained and the injected B-trees are *merged* to form a new temporary B-tree (of double size), which is then injected into the next level.

### Overwrite and Delete
Overwrite is done by simply doing a new insertion.  Since search always starts from the top (level #0 ... level#*n*), newer values will be at a lower level, and thus be found before older values.  When merging, values stored in the injected tree (that come from a lower-numbered level) have priority over the contained tree.

Deletes are the same: they are also done by inserting a tombstone (a special value outside the domain of values).  When a tombstone is merged at the currently highest numbered level it will be discarded.  So tombstones have to bubble "down" to the highest numbered level before it can be removed.


## Merge Logic

The really clever thing about this storage engine is that merging is guaranteed to be able to "keep up" with insertion.   Bitcask for instance has a similar merging phase, but it is separated from insertion.  This means that there can suddenly be a lot of catching up to do.  The flip side is that you can then decide to do all merging at off-peak hours, but it is yet another thing that need to be configured.

With Fractal B-Trees; back-pressure is provided by the injection mechanism, which only returns when an injection is complete.  Thus, every 2nd insert needs to wait for level #0 to finish the required merging; which - assuming merging has linear I/O complexity - is enough to guarantee that the merge mechanism can keep up at higher-numbered levels.  

OK, I've told you a lie.  In practice, it is not practical to create a new file for each insert (injection at level #0), so we allows you to define the "top level" to be a number higher that #0; currently defaulting to #6 (32 records).  That means that you take the amortization "hit" for ever 32 inserts.

A further trouble is that merging does in fact not have completely linear I/O complexity, because reading from a small file that was recently written is faster that reading from a file that was written a long time ago (because of OS-level caching); thus doing a merge at level #*N+1*  is sometimes more than twice as slow as doing a merge at level #*N*.  Because of this, sustained insert pressure may produce a situation where the system blocks while merging, though it does require an extremely high level of inserts.  We're considering ways to alleviate this.

Merging can be going on concurrently at each level (in preparation for an injection to the next level), which lets you utilize available multi-core capacity to merge.  




