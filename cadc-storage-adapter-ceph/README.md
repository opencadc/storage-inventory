# CEPH Storage adapter (cadc-storage-adapter-ceph) 0.1.0

## S3 and RADOS implementations
The CEPH storage adapter supports various ways of communicating with a CEPH Cluster.  This adapter supports a partial (incomplete) [RADOS](https://docs.ceph.com/docs/master/rados/api/librados-intro/) 
implementation in the [org.opencadc.inventory.storage.rados](tree/master/cadc-storage-adapter-ceph/src/main/java/org/opencadc/inventory/storage/rados) package, and a full
implementation of [S3](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/welcome.html) using the S3 JDK Version 2 in the [org.opencadc.inventory.storage.s3](tree/master/cadc-storage-adapter-ceph/src/main/java/org/opencadc/inventory/storage/s3)
package.

## Requirements
* JDK 8.+
* Gradle 5+

### RADOS only
OS level packages are required.
* librados-dev
* libradosstriper-dev


## Integration tests

### Environment

Credentials and configuration are pulled from the `user.name` System Property value's `.aws` folder for S3 tests and from the `user.name` System 
Property value's `.ceph` folder for RADOS tests.  The system administrator for the storage site will need to provide a configuration file that 
can be read by the S3 and RADOS clients.

```shell script
$ cat ~/.aws/config
[default]
access_key = FOO
aws_access_key_id = BAR
region = us-east-1
```

```shell script
$ cat ~/.ceph/config
[global]
mon_host = mycephhost.com
keyring = /home/user/.ceph/keyring
client_mount_timeout = 5
```

### Execution

To run the entire suite of integration tests against the currently deployed CEPH system, just run:
```shell script
$ gradle -i clean intTest
```

Set the user to connect with.  Defaults to `System.getProperty("user.name")`:
```shell script
# Will look in /home/myotheruser (/Users/myotheruser) for the .aws and/or .ceph folders.  In the case of RADOS, it will also use "myotheruser"
# identity to connect with.
$ gradle -i -Duser.name=myotheruser clean intTest
```

Set the bucket use.  Defaults to `System.getProperty("user.name")`:
```shell script
# Buckets are not supported in RADOS, so this only applies to S3.
$ gradle -i -Dbucket.name=myotheruser clean intTest
```

| System Property name | Purpose
| :------------------- | :------- |
| `user.name`          | User to connect with. |
| `bucket.name`        | Bucket to perform actions in. |


### RADOS
The RADOS integration tests are ignored (skipped) for the time being to avoid having to install the OS level RADOS packages if one simply wants to run the S3 tests.
