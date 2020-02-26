# CEPH Storage adapter 

The CEPH storage adapter supports various ways of communicating with a CEPH Cluster. 

* (incomplete) [RADOS](https://docs.ceph.com/docs/master/rados/api/librados-intro/) implementation
* (nearly complete) [S3](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/welcome.html) implementation

## configuration

### Java System properties
```
org.opencadc.inventory.storage.s3.S3StorageAdapter.endpoint = {S3 REST API endpoint}
org.opencadc.inventory.storage.s3.S3StorageAdapter.s3bucket = {S3 bucket name or prefix}
org.opencadc.inventory.storage.s3.S3StorageAdapter.bucketLength = {length of the StorageLocation.storageBucket value}
aws.accessKeyId = {S3 accessId}
aws.secretAccessKey = {S3 secret access key}
```
The StorageLocation.storageBucket string is used to organise stored objects so that subsets (batches) can be listed for validation; the S3StorageAdapter computes a random hex string of this length so for length `n` there will be `16^n` buckets.

The same set of properties can be used to configure both S3StorageAdapter implementations: 
* the S3StorageAdapterSB will create/use a single S3 bucket named {s3bucket} directly
* the S3StorageAdapterMB will create/use up to `16^n` S3 buckets with {s3bucket] as the prefix

Clients using the StorageAdapter API will see no difference in behaviour. However: scalability and performance tests 
will be used to determine if/when each implementation makes sense.

### Test S3 

Developers running the S3 integration tests must  provide values for the above Java system properties. Developers that are
using a shared S3 backend should chose an s3bucket value that will never collide with other users of the system (e.g. {username}-test). 

To run the entire suite of integration tests against the currently deployed CEPH system, just run:
```
gradle {configure properties} intTest
```

### Test RADOS
The RADOS integration tests are currently limited and disabled (skipped) because they require native packages provided by the operating system (librados and libradosstriper). 
