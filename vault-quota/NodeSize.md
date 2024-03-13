# vault quota design/algorithms

The definitive source of content-length (file size) of a DataNode comes from the
`inventory.Artifact` table and is not known until a PUT to storage is completed.
In the case of a `vault` service co-located with a single storage site (`minoc`),
the new Artifact is visible in the database as soon as the PUT to `minoc` is
completed. In the case of a `vault` service co-located with a global SI, the new 
Artifact is visible in the database once it is synced from the site of the PUT to 
the global database by `fenwick` (or worst case: `ratik`).

## incremental DataNode size algorithm
DataNode(s) require the `bytesUsed` be set so that sizes can be output from listing 
container nodes without a join or query to the artifact table.

This is an event watcher that gets Artifact events (after a PUT) and intiates the
propagation of sizes (space used).
```
track progress using HarvestState (source, name: TBD)
incremental query for new artifacts in lastModified order
for each new Artifact:
  query for DataNode (storageID = artifact.uri)
  if Artifact.contentLength != Node.size:
    start txn
    lock datanode
    recheck size diff
    set dataNode.size
    update HarvestState
    commit txn
```

## validate DataNode vs Artifact discrepancies
These can be validated in parallel by multiple threads, subdivide work by bucket if we add
DataNode.storageBucket (== Artifact.uriBucket).

```
discrepancy: Artifact exists but DataNode does not
explanation: DataNode created, transfer negotiated, DataNode removed, transfer executed
evidence: DeletedNodeEvent exists
action: remove artifact, create DeletedArtifactEvent

discrepancy: Artifact exists but DataNode does not
explanation: DataNode created, Artifact put, DataNode deleted, Artifact delete failed
evidence: only possible with singlePool==false
action: remove artifact, create DeletedArtifactEvent

discrepancy: DataNode exists but Artifact does not 
explanation: DataNode created, Artifact never (successfully) put (normal)
evidence: DataNode.nodeSize == 0 or null
action: none

discrepancy: DataNode exists but Artifact does not
explanation: deleted or lost Artifact
evidence: DataNode.nodeSize != 0 (deleted vs lost: DeletedArtifactEvent exists)
action: lock nodes, fix dataNode and propagate delta to parent

discrepancy: DataNode.nodeSize != Artifact.contentLength
explanation: artifact written (if DataNode.size > 0: replaced)
action: lock nodes, fix DataNode and propagate delta to parent
```
Required lock order: child-parent or parent-child OK.

The most generic implementation is a merge join of two iterators (see ratik, tantar):
```
Iterator<Artifact> aiter = artifactDAO.iterator(vaultNamespace, bucket);  // uriBucket,uri order
Iterator<DataNode> niter = nodeDAO.iterator(bucket);      // storageBucket,storageID order 
```

## database changes required
note: all field and column names TBD
* add `transient Long bytesUsed` to ContainerNode and DataNode
* add `bytesUsed` to the `vospace.Node` table
* add `storageBucket` to DataNode?? TBD
* add `storageBucket` to `vospace.Node` table

## cadc-inventory-db API required immediately
* incremental sync query/iterator: ArtifactDAO.iterator(Namespace ns, String uriBucketPrefix, Date minLastModified, boolean ordered)
  order by lastModified if set
* lookup DataNode by storageID: NodeDAO.getDataNode(URI storageID)
* indices to support new queries

## cadc-inventory-db API required later (tentative)
* validate-by-bucket: use ArtifactDAO.iterator(String uriBucketPrefix, boolean ordered, Namespace ns)
* validate-by-bucket: NodeDAO.dataNodeIterator(String storageBucketPrefix, boolean ordered)
* indices to support new queries

