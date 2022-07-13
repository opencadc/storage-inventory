<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">
<img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a>
<br />The Storage Inventory data model is licensed under the
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">
Creative Commons Attribution-ShareAlike 4.0 International License</a>.

# storage-dm
Storage Inventory (SI) Data Model

This module contains a <a href="http://www.ivoa.net/documents/VODML/index.html">VO-DML</a> 
description of the Storage data model and test code to validate the model using the 
<a href="https://github.com/opencadc/core/tree/master/cadc-vodml">cadc-vodml</a> tools.

Development starts with the UML diagrams and the current version may be ahead of the generated
VO-DML documentation. 

<img alt="Storage System UML - development version" style="border-width:0" 
src="src/main/resources/storage-inventory-0.6.png" />

# Artifact.uri
This URI is a globally unique logical identifier that is typically known to and may be defined by some other system 
(e.g. an Artifact.uri in CAOM or storageID in a storage-system backed VOSpace like ivo://cadc.nrc.ca/vault). 

{scheme}:{relative-path}

For all URIs, the filename would be the last component of the path. There is no explicit namespace: arbitrary sets of files
can be defined with a prefix-pattern match on uri. Such sets of files would be used in site mirroring 
policies. Normal CADC practice is to use the scheme and first path component (archive name) as a namespace.

Example usage of uri values:

**ad:{archive}/{filename}** *legacy*

**cadc:{archive}/{filename}** *new*

**mast:{path}/{filename}** *in use now*

Artifact.uri is a semantically meaningful identifier that remains unchanged when the Artifact is mirrored or synced to different sites. Local code/configuration is used to resolve this identifier to a URL for data access. For externally harvested and sync'ed CAOM artifacts, we would use the URI as-is in the Artifact.uri field.

Validation of CAOM versus storage requires querying CAOM and storage and cross-matching URIs to look for anomalies.

For vault (vospace), data nodes would have a generated identifier and use something like cadc:vault/{uuid} 
(but not containing the logical filename in VOSpace). There should be no use of the "vos" scheme in a uri so paths within the vospace never get used in storage and move/rename operations (container or data nodes) do not effect the storage system.  Validation of vault versus storage requires querying vault (and building uri values programmatically unless
vault db stores the full URI) and storage and cross-matching to look for anomalies.

# external services
Services that make up the storage site or global inventory depend on other CADC services. The registry lookup API permits caching; this is already implemented and effective. The permissions API could be designed to support caching or mirrors. The user and group APIs could be modified to support caching or mirroring. Caching: solve a problem by adding another problem.

<img alt="storage site deployment" style="border-width:0" 
src="https://raw.githubusercontent.com/opencadc/storage-inventory/master/storage-inventory-dm/docs/storage-external-services.png" />

# storage site deployment (dated)
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

# global inventory deployment (dated)
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
