# cadc-storage-adapter-ad
AD storage adapter implementation

### cadc-storage-adapter-ad.properties

The cadc-storage-adapter-ad.properties must be available to minoc or whatever system
is using this adapter.  Keys and vaults expected in this file are:

Set the root of the file system:
```
# The root of the file system (as seen by the container)
# (Note that this root (/fsroot) needs to be mapped to an external directory when using
this adapter with the minoc file service)
root = /fsroot
```

TBD - finished.