# Hanoi's Design

### Basics
If there are N records, there are in log<sub>2</sub>(N)  levels (each being a plain B-tree in a file named "A-*level*.data").  The file `A-0.data` has 1 record, `A-1.data` has 2 records, `A-2.data` has 4 records, and so on: `A-n.data` has 2<sup>n</sup> records.

In "stable state", each level file is either full (there) or empty (not there); so if there are e.g. 20 records stored, then there are only data in filed `A-2.data` (4 records) and `A-4.data` (16 records).

OK, I've told you a lie.  In practice, it is not practical to create a new file for each insert (injection at level #0), so we allows you to define the "top level" to be a number higher that #0; currently defaulting to #5 (32 records).  That means that you take the amortization "hit" for ever 32 inserts.

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

A further trouble is that merging does in fact not have completely linear I/O complexity, because reading from a small file that was recently written is faster that reading from a file that was written a long time ago (because of OS-level caching); thus doing a merge at level #*N+1*  is sometimes more than twice as slow as doing a merge at level #*N*.  Because of this, sustained insert pressure may produce a situation where the system blocks while merging, though it does require an extremely high level of inserts.  We're considering ways to alleviate this.

Merging can be going on concurrently at each level (in preparation for an injection to the next level), which lets you utilize available multi-core capacity to merge.


```
ABC are data files at a given level
  A oldest
  C newest
  X is being merged into from [A+B]

  270     76 [AB X|ABCX|AB X|ABCX|ABCX|ABCX|ABCX|ABCX|A   |    |    |    |    |    |    |    |    |    |
  271     76 [ABCX|ABCX|AB X|ABCX|ABCX|ABCX|ABCX|ABCX|A   |    |    |    |    |    |    |    |    |    |
  272     77 [A   |AB X|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|A   |    |    |    |    |    |    |    |    |    |
  273     77 [AB X|AB X|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|A   |    |    |    |    |    |    |    |    |    |
  274     77 [ABCX|AB X|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|A   |    |    |    |    |    |    |    |    |    |
  275     78 [A   |ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|A   |    |    |    |    |    |    |    |    |    |
  276     78 [AB X|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|A   |    |    |    |    |    |    |    |    |    |
  277     79 [ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|A   |    |    |    |    |    |    |    |    |    |
  278     79 [ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|  C |AB  |    |    |    |    |    |    |    |    |    |
  279     79 [ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|  C |AB X|    |    |    |    |    |    |    |    |    |
  280     79 [ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|A   |AB X|    |    |    |    |    |    |    |    |    |
  281     79 [ABCX|ABCX|ABCX|ABCX|ABCX|ABCX|  C |AB  |AB X|    |    |    |    |    |    |    |    |    |
  282     80 [ABCX|ABCX|ABCX| BC |AB  |AB  |AB X|AB X|AB X|    |    |    |    |    |    |    |    |    |
  283     80 [ABCX|ABCX|ABCX|  C |AB X|AB  |AB X|AB X|AB X|    |    |    |    |    |    |    |    |    |
  284     80 [A   |AB X|AB X|AB X|AB X|AB X|AB X|AB X|AB X|    |    |    |    |    |    |    |    |    |
  285     80 [AB X|AB X|AB X|AB X|AB X|AB X|AB X|AB X|AB X|    |    |    |    |    |    |    |    |    |
  286     80 [ABCX|AB X|AB X|AB X|AB X|AB X|AB X|AB X|AB X|    |    |    |    |    |    |    |    |    |
  287     80 [A   |ABCX|AB X|AB X|AB X|AB X|AB X|AB X|AB X|    |    |    |    |    |    |    |    |    |
```


When merge finishes, X is moved to the next level [becomes first open slot, in order of A,B,C], and the files merged (AB in this case) are deleted. If there is a C, then that becomes A of the next size.
When X is closed and clean, it is actually intermittently renamed M so that if there is a crash after a merge finishes, and before it is accepted at the next level then the merge work is not lost, i.e. an M file is also clean/closed properly. Thus, if there are M's that means that the incremental merge was not fast enough.

ABC files have 2^level KVs in it, regardless of the size of those KVs. XM files have 2^(level+1) approximately ... since tombstone merges might reduce the numbers or repeat PUTs of cause.

### File Descriptors
Hanoi needs a lot of file descriptors, currently   6*⌈log<sub>2</sub>(N)-TOP_LEVEL⌉, with a nursery of size 2<sup>TOP_LEVEL</sup>, and N Key/Value pairs in the store.   Thus, storing 1.000.000 KV's need 72 file descriptors, storing 1.000.000.000 records needs 132 file descriptors, 1.000.000.000.000 records needs 192.
