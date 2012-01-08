> **NOTE** I erroneously called this engine `fractal_btree` when first released, but I have since learned that what we have implemented is indeed much less sophisticated than Tokutek's Fractal Tree&reg;.   Hence, the name change to `lsm_btree`.

# LSM B-Tree Storage

This Erlang-based storage engine implements &ldquo;Log-structured merge-trees&rdquo; &mdash; a storage structure described in [this paper](http://staff.ustc.edu.cn/~jpq/paper/flash/1996-The%20Log-Structured%20Merge-Tree%20%28LSM-Tree%29.pdf).

The storage engine may eventually provide an alternative to Basho Bitcask and Google's LevelDB.  Of those two, `lsm_btree` is closer to LevelDB in operational characteristics, except it uses fewer file descriptors than LevelDB, is not as performant, but it is implemented in just ~1000 lines of Erlang.  For some the benefit of having a clean and simple Erlang implementation make it worth it.

Here's the bullet list:

- Very fast writes and deletes,
- Reasonably fast reads (N records are stored in log<sub>2</sub>(N) B-trees),
- Operations-friendly "append-only" storage (allows you to backup live system, and crash-recovery is very simple)
- The cost of evicting stale key/values is amortized into insertion, so you don't need to schedule merge to happen at off-peak hours. 
- Will support range queries (and thus eventually Riak 2i.)
- Doesn't need a boat load of RAM
- All in 1000 lines of pure Erlang code

Once we're a bit more stable, we'll provide a Riak backend.

### LSM B-Trees vs. Fractal Trees
LSM B-Trees bear some resemblance with so-called "Fractal Trees&reg;", but LSM B-Trees are much simpler.  For instance, our LSM B-Tree does not use any in-place updating (in a Fractal Tree, inner nodes have a buffer space which is updated-in place); also we use bloom filters rather than fractional cascading to speed up multiple B-tree lookups.  From published online documents, it does indeed look like fractal trees can be *much* faster (I have not run any performance tests myself).

You can read more about Fractal Trees in this slide deck from [Tokutek](http://www.tokutek.com/2011/11/how-fractal-trees-work-at-mit-today/), a company providing a MySQL backend based on Fractal Trees.  They also own some patents related to fractal trees.  I have not tried their TokuDB, but it looks truly amazing; I recommend that you try it out if you're hitting the limits of your current MySQL setup.

## How a LSM-BTree Works

If there are N records, there are in log<sub>2</sub>(N)  levels (each being a plain B-tree in a file named "A-*level*.data").  The file `A-0.data` has 1 record, `A-1.data` has 2 records, `A-2.data` has 4 records, and so on: `A-n.data` has 2<sup>n</sup> records.

In "stable state", each level file is either full (there) or empty (not there); so if there are e.g. 20 records stored, then there are only data in filed `A-2.data` (4 records) and `A-5.data` (16 records).

### Lookup
Lookup is quite simple: starting at `A-0.data`, the sought for Key is searched in the B-tree there.  If nothing is found, search continues to the next data file.  So if there are *N* levels, then *N* disk-based B-tree lookups are performed.  Each lookup is "guarded" by a bloom filter to improve the likelihood that disk-based searches are only done when likely to succeed.

### Insertion
Insertion works by a mechanism known as B-tree injection.  Insertion always starts by constructing a fresh B-tree with 1 element in it, and "injecting" that B-tree into level #0.  So you always inject a B-tree of the same size as the size of the level you're injecting it into.

- If the level being injected into empty (there is no A-*level*.data file), then the injected B-tree becomes the contents for that level (we just rename the file). 
- Otherwise, 
    - The injected tree file is renamed to B-*level*.data;
	- The files A-*level*.data and B-*level*.data are merged into a new temporary B-tree (of roughly double size), X-*level*.data.
	- The outcome of the merge is then injected into the next level.

While merging, lookups at level *n* first consults the B-*n*.data file, then the A-*n*.data file.  At a given level, there can only be one merge operation active.

### Overwrite and Delete
Overwrite is done by simply doing a new insertion.  Since search always starts from the top (level #0 ... level#*n*), newer values will be at a lower level, and thus be found before older values.  When merging, values stored in the injected tree (that come from a lower-numbered level) have priority over the contained tree.

Deletes are the same: they are also done by inserting a tombstone (a special value outside the domain of values).  When a tombstone is merged at the currently highest numbered level it will be discarded.  So tombstones have to bubble "down" to the highest numbered level before it can be truly evicted.


## Merge Logic

The really clever thing about this storage mechanism is that merging is guaranteed to be able to "keep up" with insertion.   Bitcask for instance has a similar merging phase, but it is separated from insertion.  This means that there can suddenly be a lot of catching up to do.  The flip side is that you can then decide to do all merging at off-peak hours, but it is yet another thing that need to be configured.

With LSM B-Trees; back-pressure is provided by the injection mechanism, which only returns when an injection is complete.  Thus, every 2nd insert needs to wait for level #0 to finish the required merging; which - assuming merging has linear I/O complexity - is enough to guarantee that the merge mechanism can keep up at higher-numbered levels.  

OK, I've told you a lie.  In practice, it is not practical to create a new file for each insert (injection at level #0), so we allows you to define the "top level" to be a number higher that #0; currently defaulting to #6 (32 records).  That means that you take the amortization "hit" for ever 32 inserts.

A further trouble is that merging does in fact not have completely linear I/O complexity, because reading from a small file that was recently written is faster that reading from a file that was written a long time ago (because of OS-level caching); thus doing a merge at level #*N+1*  is sometimes more than twice as slow as doing a merge at level #*N*.  Because of this, sustained insert pressure may produce a situation where the system blocks while merging, though it does require an extremely high level of inserts.  We're considering ways to alleviate this.

Merging can be going on concurrently at each level (in preparation for an injection to the next level), which lets you utilize available multi-core capacity to merge.  




