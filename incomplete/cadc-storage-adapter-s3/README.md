# S3 Storage adapter 

## NOTE: Work on this adapter is stopped.

The S3 API has severe limitations that make a complete implementation impossible.

The S3 storage adapter supports various ways of communicating with a CEPH Cluster. 

* (partial) [S3](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/welcome.html) implementation

## configuration

The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.s3.S3StorageAdapterSB|stores files with opaque keys in a single S3 bucket|
|org.opencadc.inventory.storage.s3.S3StorageAdapterMB|stores files with opaque keys in multiple S3 buckets|

### cadc-storage-adapter-s3.properties

This library is configured via a properties file in `{user.home}/config`. For developers doing testing, it is safe to include properties for both adapters in a single file but that's probably a confusing thing to do for deployment.

For the S3StorageAdapter(s):
```
org.opencadc.inventory.storage.s3.S3StorageAdapter.endpoint = {S3 REST API endpoint}
org.opencadc.inventory.storage.s3.S3StorageAdapter.s3bucket = {S3 bucket name or prefix}
org.opencadc.inventory.storage.s3.S3StorageAdapter.bucketLength = {length of the StorageLocation.storageBucket value}
aws.accessKeyId = {S3 accessId}
aws.secretAccessKey = {S3 secret access key}
```
The StorageLocation.storageBucket string is used to organise stored objects so that subsets (batches) can be listed for validation; the S3StorageAdapter computes a random hex string of this length so for length `n` there will be `16^n` buckets.

The same set of properties can be used to configure both S3StorageAdapter implementations: 
* the S3StorageAdapterSB will create/use a single S3 bucket named {s3bucket} directly and use the {storageBucket} as a key prefix
* the S3StorageAdapterMB will create/use up to `16^n` S3 buckets with {s3bucket] as the prefix


## limitations

The S3StorageAdapter(s) cannot support put of a file without required metadata up front (contentLength and contentChecksum).

All StorageAdapter implementations are limited to 5GiB file upload. Support for larger files requires using the S3 or Swift segmented upoad feature and a facade will preobably be added to the StorageAdapter API to support that.
