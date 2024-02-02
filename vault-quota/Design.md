# vault quota design/algorithms

The definitive source of content-length (file size) of a DataNode comes from the
`inventory.Artifact` table and it not known until a PUT to storage is completed.
In the case of a `vault` service co-located with a single storage site (`minoc`),
the new Artifact is visible in the database as soon as the PUT to `minoc` is
completed. In the case of a `vault` service co-located with a global SI, the new 
Artifact is visible in the database once it is synced from the site of the PUT to 
the global database by `fenwick` (or worst case: `ratik`).

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

### ContainerNode vs child nodes discrepancies
TODO: figure out how to validate ContainerNode sizes vs sum(child sizes) in a live system

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
evidence: 
action: set nodeSize = 0

discrepancy 3: DataNode exists but Artifact does not
explanation: deleted or lost Artifact
evidence: DataNode.size != 0 (deleted vs lost: DeletedArtifactEvent exists)
action: fix nodeSize

discrepancy 4: nodeSize != Artifact.contentLength
explanation: pending/missed Artifact event
action: fix DataNode and propagate delta to parent ContainerNode (same as incremental)
```

The most generic implementation is a merge join of two iterators (see ratik, tantar):
```
Iterator<Artifact> aiter = artifactDAO.iterator(vaultNamespace, bucket);  // artifact.uri order
Iterator<DataNode> niter = nodeDAO.iterator(vaultNamespace, bucket);      // storageID order 
```

## database changes required
note: all field and column names TBD
note: fields in Node classes probably not transient but TBD
* add `nodeSize` and `delta` fields to ContainerNode
* add `nodeSize` field to DataNode (no size props in LinkNode!)
* add `nodeSize` to the `vospace.Node` table
* add `delta` to the `vospace.Node` table
* add `storageBucket` to DataNode
* add `storageBucket` to `vospace.Node` table (validation)
## cadc-inventory-db API required
* incremental sync query/iterator: ArtifactDAO.iterator(Namespace ns, String uriBucketPrefix, Date minLastModified)?
* lookup DataNode by storageID: NodeDAO.getDataNode(URI storageID)?
* validate-by-bucket: use ArtifactDAO.iterator(String uriBucketPrefix, boolean ordered, Namespace ns)
* validate-by-bucket: NodeDAO.dataNodeIterator(String storageBucketPrefix, boolean ordered)
* indices to support new queries

