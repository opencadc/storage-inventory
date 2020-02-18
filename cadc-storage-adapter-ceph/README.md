# CEPH Storage adapter (cadc-storage-adapter-ceph) 0.2

The CEPH storage adapter supports various ways of communicating with a CEPH Cluster. 

(incomplete) [RADOS](https://docs.ceph.com/docs/master/rados/api/librados-intro/) implementation
(nearly complete) [S3](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/welcome.html) implementation

## configuration

### catalina.properties
```
org.opencadc.inventory.storage.s3.S3StorageAdapter.endpoint = {S3 REST API endpoint}
org.opencadc.inventory.storage.s3.S3StorageAdapter.bucketLength = {length of the StorageLocation.storageBucket value}
aws.accessKeyId = {S3 accessId}
aws.secretAccessKey = {S3 secret access key}
```
The StorageLocation.storageBucket string is used to organise stored objects so that subsets (batches) can be listed for validation; the S3StorageAdapter computes a random hex string of this length so for length n there will be 16^n buckets.
These are currently implemented as actual S3 buckets (with a prefix as described below) but this is going to change.

### Test S3 

By default, the S3 integration tests use a prefix that includes the value of `System.getProperty("user.name")` so that
different developers do not collide in a shared ceph system.

To run the entire suite of integration tests against the currently deployed CEPH system, just run:
```
gradle {set system props from config above} intTest
```

### Test RADOS
The RADOS integration tests are currewntly disabled (skipped) because they require native packages provided by the operating system (librados and libradosstriper).
