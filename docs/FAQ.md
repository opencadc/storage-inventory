# Frequently Asked Questions

Well, this is kind of a dumping ground for notes, patterns, ideas, and sometimes incomplete 
thoughts... but some are posed in the form of a question.

## file operations
Basic put/get/update/delete operations:
- put: PUT /srv/files/{uri}
- get: GET /srv/files/{uri}
- upate: POST /srv/files/{uri}
- delete: DELETE /srv/files/{uri}
- mirroring policy at sites will (mostly) be based on information in the Artifact.uri (chose wisely)
- cannot directly access vault files at a site - must use transfer negotiation with vault (no real change)
- POST can modify mutable metadata: Artifact.uri (rename), Artifact.contentType, Artifact.contentEncoding


## locate 
Files are located and accessed using transfer negotiation:
- GET: negotiate with global, locate available copies, return URL(s), order by proximity
- PUT: negotiate with global, locate writable sites, return URL(s), order by proximity -- try to match policy? (like--)
- POST and DELETE: negotiate with global, locate available copies at writable sites, return URL(s), order by proximity
- POST and DELETE (alt): modify|delete Artifact in global? update Artifact.lastModifed|delete and creates DeletedArtifactEvent, sites harvest
- **eventual consistency**: if caller wants to put *then* get a file the client must be aware of eventual consistency
- global: implement heartbeat check with sites so negotiated transfers likely to work
- vault transfer negotiation maps vos {path} to DataNode.uuid and then negotiates with global (eg, for Artifact.uri ~ vault:{uuid})
- vault implementation could maintain it's own global inventory of vault files (policy)

## curl-wget
How does a curl/wget user download a file without knowing where it is?
- an endpoint like the files endpoint that supports GET only can be implemented as part of global deployment
- this is a convenience feature so it is not part of the core design (just for clarity)
- a simple "I'm feeling lucky" implementation could just redirect to the highest ranked negotiated transfer

## overwrite a file 
Overwriting a file at storage site: atomic delete + create
- create DeletedArtifactEvent with old UUID
- create a new Artifact (new UUID), Artifact.lastModified/metaChecksum must change, global harvests, sites harvest
- before global harvests: eventually consistent (but detectable by clients in principle)
- after global harvests: consistent (only new file accessible, sites limited until sync'ed
- direct access: other storage sites would deliver previous version of file until they sync
- sequence of events: DeletedArtifactEvent.lastModified <= new Artifact.lastModified -> metadata-sync tries to process events in order
- **race condition** - put at two different sites -> two Artifact(s) with same uri but different id: resolve collision by keeping the one with max(Artifact.contentLastModified)

## propagate delete 
How does delete file get propagated?
- delete from any site with a copy; delete harvested to global and then to other sites
- process harvested delete by Artifact.id so it is decoupled from put new file with same name
- **race condition** - delete and put at different sites -> the put wins by nature 
- **eventual consistency**: if caller wants to put *then* delete a file the client must be aware of eventual consistency
- delete by Artifact UUID is idempotent so duplicate DeletedArtifactEvent records are ok

## lastModified updates
How does global learn about copies at sites other than the original?
- WAS: when a site file-sync completes (adds a local StorageLocation): update the Artifact.lastModified
- NOW: when a site file-sync completes (adds a local StorageLocation): create a
StorageLocationEvent with the Artifact.id
- when global metadata-sync from site(s) see this and tracks: add a SiteLocation
- when global metadata-sync *first* sees an Artifact: update Artifact.lastModified to
make sure the events in global are inserted at head of sequence
- metadata-sync never modifies Entity metdata: id, metaChecksum never change during sync

## cache site
How would a temporary cache instance at a site be maintained?
- site could accept writes to a "cache" instance for Artifact(s) that do not match local policy
- site metadata-validate can delete once the artifact+file are synced to sites where it does match policy: global has multiple SiteLocation(s)
- metadata-validate removed the Artifact and creates a DeletedStorageLocationEvent; file-validate cleans up local copy of file
- files could sit there orphaned if no one else wants/copies them
- a pure cache site is simply a site with local policy of "no files" and does not need to run metadata-sync w/ global

## metadata-validate
How does global inventory validate vs a storage site?  how does a storage site validate vs global?
- validation is a *set* operation between equivalent sets: local vs remote
- validation can only repair the local set
- validate subsets of artifacts (in parallel) using Artifact.uriBucket prefix
- validate ordered streams to minimise memory requirements (large sets)

**Local L** L is the set of artifacts in the local database; if L is global, it is the subset that
should match R.

**Remote R**: This set is the artifacts in the remote site that match the current filter policy.

The approach is to iterate through sets L and R, look for discrepancies, and fix L. There are only 
minor differences if validating global L.

*discrepancy*: artifact in L && artifact not in R

    explanation0: filter policy at L changed to exclude artifact in R
    evidence: Artifact in R without filter && remoteArtifact.lastModified < remote-query-start
    action: if L==storage, check num_copies>1 in R, delete Artifact, create DeletedStorageLocationEvent
            if L==global, remove siteID from Artifact.siteLocations (see note below)

    explanation1: deleted from R, pending/missed DeletedArtifactEvent in L
    evidence: DeletedArtifactEvent in R 
    action: put DAE, delete artifact
    
    explanation2: L==global, deleted from R, pending/missed DeletedStorageLocationEvent in L
    evidence: DeletedStorageLocationEvent in R 
    action: remove siteID from Artifact.siteLocations (see below)

    explanation3: L==global, new Artifact in L, pending/missed Artifact or sync in R
    evidence: ?
    action: remove siteID from Artifact.siteLocations (see note below)
    
    explanation4: L==storage, new Artifact in L, pending/missed new Artifact event in R
    evidence: ?
    action: none
    
    explanation5: TBD

    explanation6: deleted from R, lost DeletedArtifactEvent
    evidence: ?
    action: if L==global, assume explanation3
    
    explanation7: L==global, lost DeletedStorageLocationEvent
    evidence: ?
    action: assume explanation3
    
    note: when removing siteID from Artifact.siteLocations in global, if the Artifact.siteLocations becomes empty:
        the artifact should be deleted; deletion when siteLocations becomes empty due to DeletedStorageLocationEvent
        must not generate a DeletedArtifactEvent. Pro: system always reaches same state no matter what order events
        are propagated. Con: system silently "loses" artifacts in global if something is malfunctioning and it will
        be hard to notice; if metadata-sync and metadata-validate do not remove the artifact, you end up with 
        artifacts with no siteLocations - aka zero known copies - but that might be better/desirable.

*discrepancy*: artifact not in L && artifact in R

    explantion0: filter policy at L changed to include artifact in R
    evidence: ?
    action: equivalent to missed Artifact event (explanation3/4 below)
    
    explanation1: deleted from L, pending/missed DeletedArtifactEvent in R
    evidence: DeletedArtifactEvent in L
    action: none

    explanation2: L==storage, Artifact removed from L
    evidence: DeletedStorageLocationEvent in L
    action: insert Artifact, remove DeletedStorageLocationEvent 

    explanation3: L==storage, new Artifact in R, pending/missed new Artifact event in L
    evidence: ?
    action: insert Artifact
    
    explanation4: L==global, new Artifact in L, stale Artifact in R
    evidence: artifact in L without siteLocation constraint
    action: resolve as ID collision
    
    explanation5: L==global, new Artifact in R, pending/missed Artifact or StorageLocationEvent event in L
    evidence: ?
    action: insert Artifact and/or add siteLocation
    
    explanation6: deleted from L, lost DeletedArtifactEvent
    evidence: ?
    action: assume explanation3 or 5
    
    explanation7: L==storage, deleted from L, lost DeletedStorageLocationEvent
    evidence: ?
    action: assume explanation3

*discrepancy*: artifact.uri in both && artifact.id mismatch (collision)

    explantion1: same ID collision due to race condition that metadata-sync has to handle
    evidence: no more evidence needed
    action: pick winner, create DeletedArtifactEvent for loser, delete loser if it is in L, insert winner if winner was in R

*discrepancy*: artifact in both && valid metaChecksum mismatch

    explanation1: pending/missed artifact update in L
    evidence: local artifact has older Entity.lastModified indicating an update to optional metadata at remote
    action: put Artifact
    
    explanation2: pending/missed artifact update in R
    evidence: local artifact has newer Entity.lastModified indicating the update happened locally
    action: do nothing
    
*discrepancy*: artifact in both && siteLocations does not include R (L==global)

    explanation: pending/missed new Artifact event from storage site (after file-sync)
    evidence: remote siteID not in local Artifact.siteLocations
    action: add siteID to Artifact.siteLocations

## file-validate
How does a storage site validate vs local storage system?
- compare List of Artifact+StorageLocation vs storage content using storageID
- use storageBucket prefix to batch validation
- use sorted iterator to stream (merge join) for validation, order by StorageLocation
- if file in inventory & not in storage: pending file-sync job
- if file in storage and not in inventory && can generate Artifact.uri: query global for existing Artifact, create StorageLocation, maybe create Artifact
- if file in storage and not in inventory && cannot generate Artifact.uri: delete from storage? alert operator?
- if file in storage and inventory: compare checksum and length && act on discrepancies

## bit-rot
What happens when a storage site detects that a Artifact.contentChecksum != storage.checksum?
- depends on whether the storage checksum matches bytes or is simply an attached attribute
- if attached attr: can be used to detect that file was overwritten but inventory update failed
- if actual storage checksum: how to disambiguate bit-rot from update inconsistency?
- if copies-in-global: delete the Artifact and the stored file, fire DeletedStorageLocationEvent, re-sync w.r.t policy?
- if !copies-in-global: mark it bad && human intervention

## should harvesting detect if site Artifact.lastModified stream is out of whack?
- non-monotonic, except for volatile head of stack?
- clock skew

## known inefficiences
- onced ratik validates artifacts up to timestamp T, fewnick continues to process the 
sequence of artifacts as usual (redundantly for Artifact.lastModified < T)... investigate
fenwick-time-skip
- building a redundant global inventory for N storage sites currently requires
N fenwick/ratik instances that know about all the sites... investigate global-hot-spare
- global cannot filter (on Artifact.uri) when syncing DeletedArtifactEvent,
DeletedStorageLocationEvent, and StorageLocationEvent(s), so a limited global with 
a restrictive filter policy has to process all such events and ends up ignoring many
- no mechanism to expunge old events from databases: DeletedArtifactEvent from all, 
DeletedStorageLocationEvent and StorageLocationEvent from storage sites... investigate old-event-cleanup
and revisit fenwick logic to skip deleted events when starting fresh

# storage back end implementation notes
The cadc-storage-adapter API places requirements on the implementation:

0. ~fixed overhead to access a stored file (put, get)... scales moderately at best with number of files stored (indexed)
1. store (via put) and return (via iterator) metadata (min: Artifact.uri, Artifact.contentChecksum, Artifact.contentLength)
2. update metadata after a write: checksum not known before write, update Artifact.uri (rename)
3. streaming write: content length not known before write
4. consistent timestamp: contentLastModfied from put and iterator
5. support random access: resumable download, metadata extraction (fhead, fwcs), cutouts
6. support iterator (ordered by StorageLocation) *or* batched list (by storageBucket): **preferably both**

Y=yes S=scalability issues N=not possible

|feature|opaque filesystem|mountable fs (RO)|mountable fs (RW)|ceph+rados|ceph+S3|ceph+swift|AD + /data|
|-------|:---------------:|:---------------:|:---------------:|:--------:|:-----:|:--------:|:--------:|
|fixed overhead         |Y |Y?|Y?|Y |Y |Y|Y|
|store/retrieve metadata|Y |Y |Y |? |Y |Y|Y|
|update metadata        |Y |Y |Y |? |X |Y|N|
|streaming write        |Y |Y |Y |? |N |Y|Y|
|consistent timestamp   |Y |Y |Y |? |S?|Y|Y|
|random access          |Y |Y |Y |Y?|Y?|Y|Y|
|iterator               |Y |S |S |? |Y |Y|Y|
|prefix iterator        |Y |S |S |? |Y |Y|N|
|batch iterator         |Y |S |S |? |Y |Y|Y|
|prefix batch iterator  |Y |S |S |? |Y |Y|N|

For opaque fs: random hierarchical "hex" bucket scheme can give many buckets with few files, so scalability comes from
using the directory structure (buckets) to maintain finite memory footprint. The hex bucket scheme is implementing a B-tree scheme with directories (more or less).

For mountable RO filesystem: Artifact.uri structure maps to directories and files is only as scalable as is implied by 
the Artifact.uri organisation; validation (iterator) is not scalable (NS) for current practices (too flat).

For mountable RW filesystem (e.g. cavern): in addition to RO issues above, simple operations in the filesystem (mv) can
invalidate an arbitrary number of storageID values, make all those artifacts inaccessible, and cause file-validate to do 
an arbitrary amount of work to fix the storageID values. It is not feasible to avoid treating it as a new Artifact and
DeletedArtifactEvent that gets propagated to all sites unless you trust the Artifact.contentChecksum to be a unique 
and reliable identifier. Also, files created via the FS would not have metadata (checksum) so that would have to be computed
out of band and the iterator/list implementation would have to not expose the file until that happened.

For ceph+rados: native code is required so not all features were explored in detail.

For ceph+S3: the S3 API requires content-length before a write and treats stored objects as immutable so you cannot update
metadata once an object is written. Ceph+S3 also has a maximum size for writing an object (5GiB), after which clients must
use the multi-part upload API... that would have to be exposed to clients.

For ceph+swift: the Swift API allows changes to stored object metadata and allows streaming,. although still subject to a 5GiB
default object size limit before multi-part API must be used.

For AD: storageBucket = {archive} so there is no control over batch size via prefixing the bucket.


