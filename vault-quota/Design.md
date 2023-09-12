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

## DataNode size algorithm:
This is an event watcher that gets Artifact events (after a PUT) and intiates the
propagation of sizes (space used).
```
track progress using HarvestState (source: `db:{bucket range}`, name: TBD)
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
Optimization: The above sequence does the first step of propagation from DataNode to 
parent ContainerNode so the maximum work can be done in parallel using bucket ranges 
(smaller than 0-f). It also means the propagation below only has to consider 
ContainerNode.delta since DataNode(s) never have a delta.

## ContainerNode size propagation algorithm:
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

## validation

### DataNode vs Artifact discrepancies
These can be validated in parallel by multiple threads, subdivide work by bucket.

```
discrepancy 1: Artifact exists but DataNode does not
explanation: DataNode created, transfer negotiated, DataNode removed, transfer executed
evidence: check for DeletedNodeEvent
action: remove artifact, create DeletedArtifactEvent
else: ??

discrepancy 2: DataNode exists but Artifact does not
explanation: DataNode created, Artifact never (successfully) put
evidence: dataNode.size == 0
action: none

discrepancy 3: DataNode exists but Artifact does not
explanation: deleted or lost Artifact
evidence: DataNode.size != 0 (deleted vs lost: DeletedArtifactEvent exists)
action: fix DataNode.size

discrepancy 4: DataNode.size != Artifact.contentLength
explanation: pending/missed Artifact event
action: fix DataNode and propagate delta to parent ContainerNode (same as incremental)
```

This could be accomplished with a single query on on inventory.Artifact full outer join 
vospace.Node to get all the pairs. The more generic approach would be to do a merge join 
of two iterators:

Iterator<Artifact> aiter = artifactDAO.iterator(vaultNamespace, bucket);
Iterator<DataNode> niter = nodeDAO.iterator(vaultNamespace, bucket);

The more generic dual iterator approach could be made to work if the inventory and vospace 
content are in different PG database or server - TBD.

## database changes required
note: all field and column names TBD
* add `size` and `delta` fields to ContainerNode (transient)
* add `size` field to DataNode (transient)
* add `size` to the `vospace.Node` table
* add `delta` to the `vospace.Node` table
* add `storageBucket` to `vospace.Node` table (validation)
* incremental sync query/iterator (ArtifactDAO?)
* lookup DataNode by storageID (ArtifactDAO?)
* indices to support new queries

