# Swift Storage adapter 

The Swift storage adapter supports various ways of communicating with a CEPH Cluster using the Swift API. 

* [Swift](https://docs.ceph.com/docs/master/radosgw/swift/) implementation
## configuration

The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.swift.SwiftStorageAdapter|stores files with opaque keys via OpenStack SWIFT API|

This class name will be used to configure critwall, minoc, and tantar at a CEPH storage site. There
are currently no configuration options that would differ when using this adapter with different tools
at a storage site, so the configuration file (see below) should be identifical.

### cadc-storage-adapter-swift.properties

This library is configured via a properties file in `{user.home}/config`.
```
# configure storage usage
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.bucketLength={length of generated StorageLocation.storageBucket}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.bucketName={Swift container}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.multiBucket={false|true}

# configure connection to back end storage
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.authEndpoint={Swift auth v1.0 endpoint}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.username={Swift API username to authenticate}
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.key={Swift API key to authenticate}

# optional preservation
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.preserveNamespace = {namespace}
```

## multiBucket: true or false
In all cases, the `StorageLocation.storageBucket` value is a random hex string of length `bucketlength`;
this will dynamically assign up to 16^{bucketLength} logical buckets.

With multiBucket=false, a single bucket with the specified `bucketName` will be used to store all objects. The logical
bucket (hex string) will be used as part of the prefix for the object ID so that searching by prefix is efficient.

With multiBucket=true, buckets will be dynamically created with the specified `bucketName` (as a prefix) and the
logical bucket name, so this eventually creates 16^{bucketLength} real buckets in CEPH (~evenly populated). 

For example, with `bucketName=my-bucket` and `bucketLength=3` the adapter will eventually create 4096 buckets with names from `my-bucket-000` to `my-bucket-fff`. That should be sufficient for 1 billion files (~256K per bucket).

The optional `preserveNamespace` key configures the storage adapter to preserve the file
content in storage and simply mark it as deleted rather than really deleting. Multiple values may be provided by including multiple property settings in order to preserve multiple
namespace(s). The namespace value(s) must end with a colon (:) or slash (/) so one namespace
cannot accidentally match (be a prefix of) another namepsace. Example:
```
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.preserveNamespace = cadc:
org.opencadc.inventory.storage.swift.SwiftStorageAdapter.preserveNamespace = test:KEEP/
```

## limitations
It is generally a bad idea to create other buckets in the same account and a really bad idea to make other
buckets that start with `bucketName` - that is highly likely to break things horribly and cause a dumpster fire.
