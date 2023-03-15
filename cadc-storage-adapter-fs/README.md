# cadc-storage-adapter-fs
File system storage adapter implementation(s)

### cadc-storage-adapter-fs.properties

The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter|stores files in an opaque structure in the filesystem, requires POSIX extended attribute support, iterator: scalable|

TODO: finish implementation of FileSystemStorageAdapter and rename to something more specific.

```
org.opencadc.inventory.storage.fs.baseDir = {absolute path to base directory}
org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.bucketLength = {random storageBucket length}
org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.preserveNamespace = {namespace}

TODO: org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.purgeNamespace = {namespace}
TODO: org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.purgeAfter = {amount of time}
```

All StorageAdapter implementations use the same key for the base directory. The keys for specific classes are applicable to and required for that StorageAdapter implementation only.

The `OpaqueFileSystemStorageAdapter.bucketLength` key configures the length of randomly generated storage buckets (depth of directory structure). Since hex characters are used, there are 16
directories at each level and `2^length` at the bottom (where files are stored). For example, a length of 2 creates 16 directories with 16 sub-directories each, for a total of 256 leaf directories
to hold files. When validating, the listing of all files in a bottom level are loaded into memory so
the `bucketLength` needs to be large enough so the bottom level sub-directories hold a few thousand files. For example, to store one million files would need `bucketLength` of 2 (1e6/256 ~ 4000 files per directory) or 3 (1e6/4096 ~ 250 files/directory) while one billion files would require a value like 5 (1e6 directories with 1000 files each).

The optional `preserveNamespace` key configures the storage adapter to preserve the file
content in storage and simply mark it as deleted rather than really deleting. Multiple values may be provided by including multiple property settings in order to preserve multiple
namespace(s). The namespace value(s) must end with a colon (:) or slash (/) so one namespace
cannot accidentally match (be a prefix of) another namepsace. 

Example:
```
org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.preserveNamespace = cadc:
org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.preserveNamespace = test:KEEP/
```
Files where the `Artifact.uri` matches (starts with) one of these prefixes will be preserved and, in principle, recoverable. Others (e.g. `test:FOO/bar`) be permanently deleted and not recoverable.

The optional `purge` directives configure the storage adapter to perform a real deletion from storage for files
_that were previously preserved_. These directives only make sense when used by file-validate (tantar) to resolve
discrepancies between inventory and storage. The `purgeNamespace` key configures the storage adapter to delete matching files. The `purgeAfter` key configures the storage adapter to delete matching files where the file has been preserved (marked as deleted) longer than the configured value (currently, this is an integer number of days).

Example:
```
org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.preserveNamespace = cadc:
org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.purgeNamespace = cadc:OBSOLETE/
org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.purgeAfter = 730
```
Previously deleted (marked) files where the `Artifact.uri` matches (starts with) `cadc:OBSOLETE/` 
and files that were deleted (marked) more than 2 years ago will be deleted from storage. All other files
where the `Artifact.uri` matches (starts with) `cadc:` are preserved.
