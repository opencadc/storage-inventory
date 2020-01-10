# ceph-storage-adapter-ceph 0.2.0

## 2020.01.09 (0.2.0)
* Integration tests added
* RADOS and S3 packages co-exist in single `cadc-storage-adapter-ceph` module.
* Upgraded S3 driver to latest 2.10.45.
* PUTs generate UUIDs for storage IDs
* PUTs compute Bucket name (for S3) from generated Storage ID.
* PUTs add extended attributes for "md5" and "uri" values.
  * Extended Attribute "uri" preserves the human readable URI.

## 2019.12.20 (0.1.0)
* Initial working commit
* S3 (PUT/GET/LIST) tests work
* Performance tests with FITS file HDU access through RADOS and S3
* More documentation added.
* RADOS Java SDK 0.5
* S3 Java SDK 2.10.36