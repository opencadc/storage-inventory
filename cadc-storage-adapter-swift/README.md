# Swift Storage adapter 

The Swift storage adapter supports various ways of communicating with a CEPH Cluster using the Swift API. 

* [Swift](https://docs.ceph.com/docs/master/radosgw/swift/) implementation
## configuration

The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.swift.SwiftStorageAdapter|stores files with opaque keys via OpenStack SWIFT API|

This class names will be used to configure critwall, minoc, and tantar at a CEPH storage site.

### cadc-storage-adapter-swift.properties

This library is configured via a properties file in `{user.home}/config`. For developers doing testing, it is safe to include properties for both adapters in a single file but that's probably a confusing thing to do for deployment.

For the SwiftStorageAdapter:
```
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.bucketLength={length of generated StorageLocation.storageBucket}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.bucketName={Swift container}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.multiBucket={false|true}

org.opencadc.inventory.storage.swift.SwiftStorageAdapter.authEndpoint={Swift auth v1.0 endpoint}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.username={Swift API username to authenticate}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.key={Swift API key to authenticate}
```

## multiBucket: true or false
In all cases, the `StorageLocation.storageBucket` value is a random hex string of length `bucketlength`; this will
dynamically assign up to 16^{bucketLength} logical buckets.

With multiBucket=false, a single bucket with the specified `bucketName` will be used to store all objects. The logical
bucket (hex string) will be used as part of the prefix for the object ID so that searching by prefix is efficient.

With multiBucket=true, buckets will be dynamically created with the specified `bucketName` (as a prefix) and the
logical bucket name, so this eventually creates 16^{bucketLength} real buckets in CEPH (~evenly populated). 

For example, with `bucketName=my-bucket` and `bucketLength=3` the adapter will eventually create 4096 buckets with names from `my-bucket-000` to `my-bucket-fff`. That should be sufficient for 1 billion files (~256K per bucket).

## limitations
The Swift StorageAdapter is currently limited to 5GiB file upload. Support for larger files requires using the  segmented upoad feature and a facade will be added to the StorageAdapter API to support that.

It is generally a bad idea to create other buckets in the same account and a really bad idea to make other
buckets that start with `bucketName` - that is highly likely to break things horribly and cause a dumpster fire.
