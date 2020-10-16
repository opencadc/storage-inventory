<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">
<img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a>
<br />The Common Archive Observation Model is licensed under the
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">
Creative Commons Attribution-ShareAlike 4.0 International License</a>.

# high level features

arbitrary organisation of archive files (File.uri)
- file organisation supports mirroring policy

one or more independent global inventory(ies) of all files and their locations
- specialised global inventory(ies) can be created (eg vault can have it's own copy of global)

one or more storage sites with local inventory and data storage
- arbitrary mirroring policies at each site
- different back end storage implementation (assumed off-the-shelf)

incremental metadata propagation and robust metadata validation
- site to global
- global to site

scalable validation using uri, uriBucket
- CAOM vs storage
- vault vs storage
- global vs site
- site vs global

multiple storage backends in use at any one time

# transition plan features

no throw-away transition tools or development (CT: continuous transition)

no "big switch": 
- start with AD site
- operate with AD in parallel with other (types of) sites
- eventually retire AD?

# what's NOT included

quotas, allocations, resource management

monitoring, control, or config of back end storage systems
- use their tools

monitoring or control of processes and services
- use kubernetes

reporting
- get logs from containers
- query site inventory
- query global inventory

# storage-dm
Storage Inventory (SI) Data Model

This module contains a <a href="http://www.ivoa.net/documents/VODML/index.html">VO-DML</a> 
description of the Storage data model and test code to validate the model using the 
<a href="https://github.com/opencadc/core/tree/master/cadc-vodml">cadc-vodml</a> tools.

Development starts with the UML diagrams and the current version may be ahead of the generated
VO-DML documentation. 

<img alt="Storage System UML - development version" style="border-width:0" 
src="https://github.com/opencadc/storage-inventory/raw/master/storage-inventory-dm/src/main/resources/storage-inventory-0.5.png" />

# Artifact.uri
This URI is a globally unique logical identifier that is typically known to and may be defined by some other system 
(e.g. an Artifact.uri in CAOM or storageID in a storage-system backed VOSpace like ivo://cadc.nrc.ca/vault). 

{scheme}:{path}

ivo://{authority}/{name}?{path}

For all URIs, the filename would be the last component of {path}. There is no explicit namespace: arbitrary sets of files
can be defined with a prefix-pattern match on uri. Such sets of files would be used in validation and in site mirroring 
policies.

For resolvable ivo URIs, the resourceID can be extracted by dropping the query string. The resourceID 
can be found in a registry and allows clients to find data services. This form allows for generic tools to resolve
and access files from external systems. Example usage of equivalent uri values:

**ad:{archive}/{filename}** *classic*

**cadc:{path}/{filename}** *new*

**mast:{path}/{filename}** *in use now*

ivo://cadc.nrc.ca/{archive}?{path}/{filename} *resolvable archive?*

ivo://cadc.nrc.ca/{srv}?{path}/{filename} *resolvable multi-archive service?*

The fully resolvable uri (ivo scheme) can be used to extract a resourceID (up to the ?) and perform a registry lookup.
The resulting record would contain at least two capabilities: a transfer negotiation or a files endpoint and a permissions endpoint. Classic (ad) and proposed new (cadc scheme) usage is to use a shortcut scheme that is configured to be equivalent to the multi-archive data centre. The first form (with the archive in the resourceID) allows for changes from a common to a different permission service (for that archive); the second form is the resolvable version of current practice. The short forms (ad and cadc schemes) require configuration at storage sites to map the scheme (e.g. cadc -> ivo://cadc.nrc.ca/archive) for capability lookup. For externally harvested and sync'ed CAOM artifacts, we would use the URI as-is in the Artifact.uri field. For the simple form (e.g. mast:HST/path/to/file.fits) we would configure the scheme (mast) as a shortcut to the CAOM archive metadata service.

Validation of CAOM versus storage requires querying CAOM (1+ collections) and storage (single namespace) and 
cross-matching URIs to look for anomalies.

For vault (vospace), data nodes would have a generated identifier and use something like 
ivo://cadc.nrc.ca/vault?{uuid}/{filename} or vault:{uuid}/{filename}. For the latter, storage sites would require
configuration to support the shortcut (as would other instances). There should be no use of the "vos" scheme in a uri so paths within the vospace never get used in storage and move/rename operations (container or data nodes) do not effect the storage system.  Validation of vault versus storage requires querying vault (and building uri values programmatically unless
vaultdb stores the full URI) and storage and cross-matching to look for anomalies.

Since we need to support mast and vault schemes (at least), it is assumed that we would use the "cadc" scheme going 
forward and support (configure) the "ad" scheme for backwards compatibility. 

# external services
Services that make up the storage site or global inventory depend on other CADC services. The registry lookup API permits caching; this is already implemented and effective. The permissions API could be designed to support caching or mirrors. The user and group APIs could be modified to support caching or mirroring. Caching: solve a problem by adding another problem.

<img alt="storage site deployment" style="border-width:0" 
src="https://raw.githubusercontent.com/opencadc/storage-inventory/master/storage-inventory-dm/docs/storage-external-services.png" />

# storage site
A storage site tracks all files written to local storage. These files can be harvested by anyone: central inventory, 
clones, backups, etc. A storage site is fully consistent w.r.t local file operations (e.g. put + get + delete).

Storage sites:
- track writes of local files (persist File + StorageLocation)
- implement get/put/delete service API
- implement policy to sync Artifact(s) from global
- implement file sync from other sites using global transfer negotiation
- implement validation of file metadata vs storage

A storage site maintains StorageLocation->Artifact for Artifact(s) in local storage. Local policy syncs a subset of File(s)
from global; new Artifact(s) from other sites would have no matching StorageLocation and file-sync would know to retrieve
the file. Local put creates the StorageLocation directly.

<img alt="storage site deployment" style="border-width:0" 
src="https://raw.githubusercontent.com/opencadc/storage-inventory/master/storage-inventory-dm/docs/storage-site-deploy.png" />

# global inventory
A global inventory service is built by harvesting sites, files, and deletions from all known sites.
A global inventory is eventually consistent.

Global inventory (may be one or more):
- harvests StorageSite metadata from storage sites
- harvests Artifact + DeletedArtifactEvent + DeletedStorageLocationEvent from storage sites (persist Artifact + SiteLocation)
- implements transfer negotiation API

Rather than just accumulate new instances of Artifact, the harvested File may be a new Artifact or an existing Artifact 
at a new site that has to be merged into the global inventory. Thus, Artifact.lastModified in a global inventory is not 
equal to the values at individual storage sites. TBD: should global store the min(lastModified from sites)? max(lastModified from sites)? act like origin and store current timestamp?.

The metadata-sync tools could allow for global policy so one could maintain a global view of a defined set of file. 
This might be useful so vault could maintain it's own global view and not rely on an external global inventory service 
for transfer negotiation.

<img alt="global storage inventory deployment" style="border-width:0" 
src="https://raw.githubusercontent.com/opencadc/storage-inventory/master/storage-inventory-dm/docs/global-inventory-deploy.png" />

# patterns, ideas, and incomplete thoughts...

files (basic put/get/delete):
- PUT /srv/files/{uri}
- GET /srv/files/{uri}
- DELETE /srv/files/{uri}
- POST /srv/files/{uri}
- mirroring policy at sites will (mostly) be based on information in the Artifact.uri (chose wisely)
- cannot directly access vault files at a site - must use transfer negotiation (no real change)
- POST can modify the Artifact.uri (rename), the Artifact.contentType, and the Artifact.contentEncoding

locate (transfer negotiation):
- GET: negotiate with global, locate available copies, return URL(s), order by proximity
- PUT: negotiate with global? sites that are writable, try to match policy? (like--)
- DELETE: delete from global? global deletes Artifact, creates DeletedArtifactEvent, sites harvest and delete as usual
- DELETE: negotiate with global? find sites with a copy and delete from one?
- global should implement heartbeat check with sites so negotiated transfers likely to work
- vault transfer negotiation maps vos {path} to DataNode.uuid and then negotiates with global
- vault implementation could maintain it's own global inventory of vault files (policy)

how does a curl/wget user download a file?
- an endpoint like the files endpoint that supports GET only can be implemented as part of global deployment
- this is a convenience feature so it is not part of the core design (just for clarity)
- a simple "I'm feeling lucky" implementation could just redirect to the highest ranked negotiated transfer
- something with a marginally better chance of success could use have the storage site files endpoint redirect 
  failed GETs back to global, but that introduces tighter coupling between global and sites (like--)

overwrite a file at storage site: atomic delete + create
- create a new Artifact (new UUID), Artifact.lastModified/metaChecksum must change, global harvests, sites harvest
- create DeletedArtifactEvent with old UUID
- before global harvests: eventually consistent (but detectable by clients in principle)
- after global harvests: consistent (only new file accessible, sites limited until sync'ed
- direct access: other storage sites would deliver previous version of file until they sync
- race condition when put at two sites -> two Artifact(s) with same uri but different id: keep max(Artifact.lastModified)

how does delete file get propagated?
- delete from any site with a copy; delete harvested to global and then to other sites
- process harvested delete by Artifact.id so it is decoupled from put new file with same name
- race condition - delete and put at different sites: the put always wins
- delete by Artifact UUID is idempotent so duplicate DeletedArtifactEvent records are ok

how does global learn about copies at sites other than the original?
- when a site syncs a file (adds a local StorageLocation): update the Artifact.lastModified
- global metadata-sync from site(s) will see this and add a SiteLocation
- global does not update Artifact.lastModified?
- the Artifact.id and Artifact.metaChecksum never change during sync
- global's view of the Artifact.lastModified will be the latest change at all sites, but that doesn't stop it from getting an "event" out of order and still merging in the new SiteLocation

how would a temporary cache instance at a site be maintained?
- site could accept writes to a "cache" instance for Artifact(s) that do not match local policy
- site could delete once global has other SiteLocation(s), update Artifact.lastModified so global will detect 
  and remove SiteLocation
- files could sit there orphaned if no one else wants/copies them
- this feature is not planned, just speculation

how does global inventory validate vs site?  how does site validate vs global (w.r.t. policy)?
- get list of Artifact(s) from the site and compare with Artifact+SiteLocation
- add missing Artifact(s)
- add missing SiteLocation(s)
- update mutable Artifact metadata (validate sees it before metadata-sync)
- remove orphaned SiteLocation(s)
- remove Artifact(s) with no SiteLocation??
- use uriBucket to do validation in batches/parallel
- use streaming (merge-join) to minimise memory requirements

how does a storage site validate vs local storage system?
- compare List of Artifact+StorageLocation vs storage content using storageID
- use storageBucket prefix to batch validation?
- use sorted iterator to stream (merge join) for validation?
- if file in inventory & not in storage: pending file-sync job
- if file in storage and not in inventory && can generate Artifact.uri: query global, create StorageLocation, maybe create Artifact
- if file in storage and not in inventory && cannot generate Artifact.uri: delete from storage? alert operator?
- if file in storage and inventory: compare checksum and length && act on discrepancies

what happens when a storage site detects that a Artifact.contentChecksum != storage.checksum?
- depends on whether the storage checksum matches bytes or is simply an attached attribute
- if attached attr: can be used to detect that file was overwritten but inventory update failed
- if actual storage checksum: how to disambiguate bit-rot from update inconsistency?
- if copies-in-global: delete the Artifact and the stored file, fire DeletedStorageLocationEvent, re-sync w.r.t policy?
- if !copies-in-global: mark it bad && human intervention

should harvesting detect if site Artifact.lastModified stream is out of whack?
- non-monotonic, except for volatile head of stack?
- clock skew

# storage back end implementation notes
The cadc-storage-adapter API places requirements on the implementation:

0. ~fixed overhead to access a stored file (put, get)... scales moderately at best with number of files stored (indexed)
1. store (via put) and return (via iterator) metadata (min: Artifact.uri, Artifact.contentChecksum, Artifact.contentLength)
2. update metadata after a write: checksum not known before write, update Artifact.uri (rename)
3. streaming write: content length not known before write
4. consistent timestamp: contentLastModfied from put and iterator/list
5. support random access: resumable download, metadata extraction (fhead, fwcs), cutouts
6. support iterator (ordered by StorageLocation) *or* batched list (by storageBucket): **preferably both**

Y=yes NS=not scalable X=not possible

|feature|opaque filesystem|mountable fs (RO)|mountable fs (RW)|ceph+rados|ceph+S3|ceph+swift|AD + /data|
|-------|:---------------:|:---------------:|:---------------:|:--------:|:-----:|:--------:|:--------:|
|fixed overhead|Y|Y?|Y?|Y|Y|Y|Y|
|store/retrieve metadata|Y|Y|Y|?|Y|Y?|Y|
|update metadata        |Y|Y|Y|?|X|Y?|X|
|streaming write        |Y|Y|Y|?|X?|Y?|Y|
|consistent timestamp   |Y|Y|Y|?|NS|?|Y|
|random access          |Y|Y|Y|Y?|Y?|Y?|Y|
|batch list             |Y|NS|NS|Y?|Y|Y?|NS|
|prefix batch list      |Y|NS|NS|Y?|Y|Y?|X|
|iterator               |Y|NS|NS|?|Y|Y?|Y|
|prefix iterator        |Y|NS|NS|?|Y|Y?|X|
|batch iterator         |Y|NS|NS|?|Y|Y?|Y|
|prefix batch iterator  |Y|NS|NS|?|Y|Y?|X|

For opaque fs: random hierarchical "hex" bucket scheme can give many buckets with few files, so scalability comes from
using the directory structure (buckets) to maintain finite memory footprint. The hex bucket scheme is implementing a B-tree scheme with directories (more or less).

For mountable RO filesystem: Artifact.uri structure maps to directories and files is only as scalable as is implied by 
the Artifact.uri organisation; validation (iterator or list) is NS for current practices (too flat).

For mountable RW filesystem (e.g. cavern): in addition to RO issues above, simple operations in the filesystem (mv) can
invalidate an arbitrary number of storageID values, make all those artifacts inaccessible, and cause file-validate to do 
an arbitrary amount of work to fix the storageID values. It is not feasible to avoid treating it as a new Artifact and
DeletedArtifactEvent that gets propagated to all sites unless you trust the Artifact.contentChecksum to be a unique 
and reliable identifier. Also, files created via the FS would not have metadata (checksum) so that would have to be computed
out of band and the iterator/list implementation would have to not expose the file until that happened.

For ceph+rados: native code is required so not all features were explored in detail.

For ceph+S3: the S3 API requires content-length before a write and treats stored objects as immutable so you cannot update
metadata once an object is written. Ceph+S3 also has a maximum size for writing an object (5GiB), after which clients *must*
use the multi-part upload API... that would have to be exposed to clients (not a bad idea, but the limit is modest).

For ceph+swift: the Swift API does not appear to treat stored objects as immutable but this has not been verified with 
working code.

For AD: storageBucket = {archive} so there is no control over batch size via prefixing the bucket.

{1}: Buckets are not a notion in `RADOS`.  They are part of the Object ID in the format 
of `{bucket_marker}_{numeric_id}.{key_id}`.  The `bucket_marker` is looked up in the `default.rgw.meta` pool by name.  This
is how the S3 endpoint is supported within Ceph.
