# cadc-storage-adapter-fs
File system storage adapter implementation(s)

### cadc-storage-adapter-fs.properties

The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter|stores files in an opaque structure in the filesystem, requires POSIX extended attribute support, iterator: scalable|
|org.opencadc.inventory.storage.fs.LogicalFileSystemStorageAdapter|stores files in a structure using the Artifact.uri values, requires POSIX extended attribute support, iterator: only as scalable as the URI organisation|

Example:
```
org.opencadc.inventory.storage.fs.baseDir = {absolute path to base directory}

org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.bucketLength = {random storageBucket length}
```

All StorageAdapter implementations use the same key for the base directory. The keys for specific classes are applicable to and required for that StorageAdapter implementation only.

The `OpaqueFileSystemStorageAdapter.bucketLength` key configures the length of randomly generated storage buckets (depth of directory structure). Since hex characters are used, there are 16 directories at each level and `16^bucketLength` at the bottom (where files are stored). For example, a length of 2
creates 16 directories with 16 sub-directories each, for a total of 256 leaf directories to hold files and the _buckets_ go from `00` to `ff`. When
validating (see `tantar`), the list of all files in a bottom level directory are loaded into memory: the `bucketLength` needs to be large enough so the bottom level sub-directories hold thousands of files each. For example, to store one million files would need `bucketLength` of 2 (1e6/256 ~ 4000 files per directory) or 3 (1e6/4096 ~ 250 files/directory) while one billion files would require a value like 5 (1e6 directories with 1000 files each).


