# cadc-storage-adapter-fs
File system storage adapter implementation

### cadc-storage-adapter-fs.properties

The cadc-storage-adapter-fs.properties must be available to minoc or whatever system
is using this adapter. The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter|stores files in an opaque structure in the filesystem, requires POSIX extended attribute support, iterator: scalable|

TODO: fix up the previous implementation's URI mode as TransparentFileSystemStorageAdapter.

```
org.opencadc.inventory.storage.fs.baseDir = {absolute path to base directory}
org.opencadc.inventory.storage.fs.OpaqueFileSystemStorageAdapter.bucketLength = {random storageBucket length}
```

All StorageAdapter implementations use the same key key for the base directory. The keys 
for specific classes are required for that StorageAdapter implementation.


