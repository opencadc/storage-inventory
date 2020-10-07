# Swift Storage adapter 

The Swift storage adapter supports various ways of communicating with a CEPH Cluster using the Swift API. 

* [Swift](https://docs.ceph.com/docs/master/radosgw/swift/) implementation
## configuration

The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.swift.SwiftStorageAdapter|stores files with opaque keys in a single Swift container|

This class names will be used to configure critwall, minoc, and tantar at a CEPH storage site.

### cadc-storage-adapter-swift.properties

This library is configured via a properties file in `{user.home}/config`. For developers doing testing, it is safe to include properties for both adapters in a single file but that's probably a confusing thing to do for deployment.

For the SwiftStorageAdapter:
```
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.bucketLength={length of generated StorageLocation.storageBucket}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.bucketName={Swift container}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.authEndpoint={Swift auth v1.0 endpoint}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.username={Swift API username to authenticate}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.key={Swift API key to authenticate}
```

## limitations

The Swift StorageAdapter is currently limited to 5GiB file upload. Support for larger files requires using the  segmented upoad feature and a facade will be added to the StorageAdapter API to support that.

