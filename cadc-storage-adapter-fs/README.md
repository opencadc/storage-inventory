# cadc-storage-adapter-fs
File system storage adapter implementation

### cadc-storage-adapter-fs.properties

The cadc-storage-adapter-fs.properties must be available to minoc or whatever system
is using this adapter.  Keys and vaults expected in this file are:

Set the root of the file system:
```
# The root of the file system (as seen by the container)
# (Note that this root (/fsroot) needs to be mapped to an external directory when using
this adapter with the minoc file service)
root = /fsroot
```

Set the bucket mode:
```
# URI (uses artifactURIs for dirs) or URIBUCKET (uses uriBucket segments as dirs)
bucketMode = <URI|URIBUCKET>
```

Set the bucket length for bucket mode URIBUCKET
```
# Only applies to bucketMode = URIBUCKET
# valid values between 0 and 5
bucketLength = 1
```
