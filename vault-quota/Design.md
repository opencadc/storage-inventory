# vault quota design/algorithms

The definitive source of content-length (file size) of a DataNode comes from the
`inventory.Artifact` table and it not known until a PUT to storage is completed.
In the case of a `vault` service co-located with a single storage site (`minoc`),
the new Artifact is visible in the database as soon as the PUT to `minoc` is
completed. In the case of a `vault` service co-located with a global SI, the new 
Artifact is visible in the database once it is synced from the site of the PUT to
`minoc` to the global database by `fenwick` (or worst case: `ratik`).

## TODO
The design below only takes into account incremental propagation of space used 
by stored files. It is not complete/verified until we also come up with a validation
algorithm that can detect and fix discrepancies in a live `vault`.

## Event watcher algorithm:
```
track progress using HarvestState (name: `Artifact`, source: `db:{bucket range}`)
incremental query for new artifacts in lastModified order
for each new Artifact:
  query for DataNode (storageID = artifact.uri)
  if Artifact.contentLength != Node.size:
    start txn
    lock datanode
    compute delta
    lock parent
    apply delta to parent.delta
    set dataNode.size
    update HarvestState
    commit txn
```
The above sequence does the first step of propagation from DataNode to parent ContainerNode.
This can be done in parallel by using bucket ranges (smaller than 0-f).

## Container size propagation algorithm:
```
query for ContainerNode with non-zero delta
for each ContainerNode:
  start txn
  lock containernode
  re-check delta
  lock parent
  apply delta to parent.delta
  apply delta containernode.size, set containernode.delta=0
  commit txn
```
The above sequence finds candidate propagations, locks (order: child-then-parent as above), 
and applies the propagation. This moves the outstanding delta up the tree one level. If the
sequence acts on multiple child containers before the parent, the delta(s) naturally
_merge_ and there are fewer larger delta propagations in the upper part of the tree. It would
be optimal to do propagations depth-first but it doesn't seem practical to forcibly accomplish 
that ordering.

Container size propagation will be implemented as a single sequence (thread). We could add
something to the vospace.Node table to support subdividing work and enable multiple threads, 
but there is nothing there right now.

## database changes required
note: all field and column names TBD
* add `size` and `delta` fields to ContainerNode (transient)
* add `size` field to DataNode (transient)
* add `size` to the `vospace.Node` table
* add `delta` to the `vospace.Node` table
* incremental sync query/iterator (ArtifactDAO?)
* lookup DataNode by storageID (ArtifactDAO?)
* indices to support new queries

