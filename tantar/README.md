# Storage Inventory file-validate process (tantar)

Process to ensure validity of the information stored in the inventory database and back end storage at a storage site is
correct. This process is intended to be run periodically at a storage site to keep the site in a valid state.

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.

### tantar.properties
```
org.opencadc.tantar.logging = {info|debug}

# set whether to report all activity or to perform any actions required.
org.opencadc.tantar.reportOnly = {true|false}

# set the bucket prefix(es) that tantar will validate
org.opencadc.tantar.buckets = {bucket prefix or range of bucket prefixes}

# set the policy to resolve conflicts of files
org.opencadc.tantar.policy.ResolutionPolicy = InventoryIsAlwaysRight | StorageIsAlwaysRight

## inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.tantar.inventory.schema={schema for inventory database objects}
org.opencadc.tantar.inventory.username={username for inventory admin pool}
org.opencadc.tantar.inventory.password={password for inventory admin pool}
org.opencadc.tantar.inventory.url=jdbc:postgresql://{server}/{database}

## storage adapter settings
org.opencadc.inventory.storage.StorageAdapter={fully-qualified-classname of implementation}

## optional recoverable after delete behaviour
org.opencadc.tantar.recoverableNamespace = {namespace}

## optional purge behaviour
org.opencadc.tantar.purgeNamespace = {namespace}

## optional full scan of storage 
org.opencadc.tantar.includeRecoverable = true | false
```
The `inventory` database account owns and manages (create, alter, drop) inventory database objects and modifies the content. 
The database is specified in the JDBC URL. Failure to connect or initialize the database will show up in logs and cause
the application to exit.

The _buckets_ value indicates a subset of inventory database and back end storage to validate. 
This uses the Artifact.storageLocation.storageBucket values so the exact usage and behaviour 
depends on the StorageAdapter being used for the site. There are several kinds of buckets in use: 
some StorageAdapter(s) use BucketType.HEX and one can prefix the hex string to denote fewer but 
larger buckets (e.g. bucket prefix "0" denotes ~1/16 of the storage locations; the prefix range "0-f" 
denotes the entire storage range). A StorageAdapter using BucketType.PLAIN has named buckets that
are used as-is. A StorageAdapter using BucketType.NONE leaves Artifact.storageLocation.storageBucket
null; the _buckets_ value in this case optional (ignored).

The _StorageAdapter_ is a plugin implementation to support the back end storage system. These are implemented in separate libraries; 
each available implementation is in a library named cadc-storage-adapter-{impl} and the fully qualified class name to use is documented 
there. Additional java system properties and/or configuration files may be required to configure the appropriate storage adapter:
- [Swift Storage Adapter](https://github.com/opencadc/storage-inventory/tree/master/cadc-storage-adapter-swift)

- [File System Storage Adapter](https://github.com/opencadc/storage-inventory/tree/master/cadc-storage-adapter-fs)

- [AD Storage Adapter](https://github.com/opencadc/storage-inventory/tree/master/cadc-storage-adapter-ad)


The _SQLGenerator_ is a plugin implementation to support the database. There is currently only one implementation that is tested with 
PostgeSQL (10+). Making this work with other database servers in future may require a different implementation.

The inventory _schema name_ is the name of the database schema used for all created database objects (tables, indices, etc). This 
currently must be "inventory" due to configuration limitations in luskan.

The _ResolutionPolicy_ is a plugin implementation that controls how discrepancies between the inventory database and the back end storage 
are resolved. The policy specifies that one is the definitive source of information about the existence of an Artifact or file and fixes 
the discrepancy accordingly. Since these policies are all implemented within `tantar`, policies are identified by the simple class name.

The standard policy one would normally use is _InventoryIsAlwaysRight_: an Artifact in the database indicates the correct state and a 
file without an Artifact should be deleted.

The _StorageIsAlwaysRight_ policy is used for a site where files are added to back end storage and "ingested" into inventory (a read-only 
storage site); this is suitable when using a StorageAdapter to migrate content from an old system. This policy will never delete stored files 
but it will delete Artifact(s) from the inventory database that do not match a stored file (and generate a DeletedArtifactEvent that will 
propagate to other sites), create new Artifact(s) for files that do not match an existing one, and may modify Artifact metadata in the 
inventory database to match values from storage. This policy makes the back end storage of this site the definitive source for the existence 
of artifacts/files.

The following StorageAdapter and ResolutionPolicy combinations are considered well tested:
```
OpaqueFilesystemStorageAdapter + InventoryIsAlwaysRight
SwiftStorageAdapter + InventoryIsAlwaysRight
AdStorageAdapter + StorageIsAlwaysRight (CADC archive migration to SI)
```

The _includeRecoverable_ configuration is optional and defaults to _false_. When true, `tantar` will 
request that the StorageAdapter include recoverable (previously deleted but preserved) stored objects for
consideration. This option is likely to make tantar validation slower if the number of previously deleted 
ojects is large because it usually requires an additional query to the inventory database for each stored 
object that dosn't currently match an artifact (which is all of the deleted/preserved stored objects). This 
option should be used rarely, but it can potentially recover from the scenario where an Artifact has no
storageLocation but the file (or an older copy with the same bytes) still resides in storage.

The optional _recoverableNamespace_ key causes tantar to configure the storage adapter so that deletions
preserve the file content in a recoverable state. This generally means that storage space remains in use
but details are internal to the StorageAdapter implementation . Multiple values may be provided by 
including multiple property settings in order to make multiple namespace(s) recoverable. The namespace
value(s) must end with a colon (:) or slash (/) so one namespace cannot _accidentally_ match (be a prefix of) 
another namespace.

EXPERIMENTAL: The optional _purgeNamespace_ key tells tantar to configure the storage adapter to perform a real deletion from
storage for matching files _even if they were previously preserved_ or match existing _recoverableNamespace_ 
configuration. **This feature is experimental and may be renamed, modified, or removed in future.** It should not be needed
or used for routine (scheduled) validation. TODO: in future, we could implement other options like a "purgeAfter" 
feature to remove matching recoverable stored objects that are sufficiently old... use cases TBD.

Example:
```
org.opencadc.tantar.recoverableNamespace = cadc:
org.opencadc.tantar.recoverableNamespace = test:KEEP/

org.opencadc.tantar.purgeNamespace = cadc:OBSOLETE/
```
Stored objects where the `Artifact.uri` matches (starts with) one of these prefixes will be recoverable. Others 
(e.g. `test:FOO/bar`) will be permanently deleted and not recoverable.

Deletion of stored objects where the `Artifact.uri` matches (starts with) `cadc:OBSOLETE/` will be 
real deletions from storage. Deletion of other objects where the `Artifact.uri` matches (starts with) `cadc:` 
or `test:KEEP/` will be preserved and recoverable (in future). By default, the deletion of objects that do not 
match any _recoverableNamespace_ namespace will be real deletions from storage. Actual deletion of objects by
`tantar` depends on the resolution policy (above). 

Note: Since artifact and stored object deletion can also be performed by requests to the `minoc` service,
all instances of `minoc` and `tantar` that use the same inventory and storage adapter should use the same
 _recoverableNamespace_ configuration so that preservation and recovery (from mistakes) is consistent.

### cadcproxy.pem
This client certificate may be used by the StorageAdapter implementation.

## building it
```
gradle clean build
docker build -t tantar -f Dockerfile .
```

## checking it
```
docker run -t tantar:latest /bin/bash
```

## running it
```
docker run -r --user opencadc:opencadc -v /path/to/external/config:/config:ro --name tantar tantar:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag tantar:latest tantar:$t
done
unset TAGS
docker image list tantar
```
