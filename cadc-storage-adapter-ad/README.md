# cadc-storage-adapter-ad
AD storage adapter implementation. 

Currently supports GET and Iterator functionality only. 
Requires credentials valid for calling the ad web service
be in the Subject. Credentials must be provided by the calling program.

This library currently provides a partial implementation of the StorageAdapter plugin API:

_org.opencadc.inventory.storage.ad.AdStorageAdapter_ 
supports get and iterator (streaming list) so can be used in a read-only minoc instance and a tantar instance with the 
_StorageIsAlwaysRight_ policy. A bucket with this adapter defined by the name of an archive in AD. `VOSpac` is
the only archive that has multiple storage buckets defined by the first 4 digits fo the contentMD5 of the file.
For example `VOSpac:0000` refers to all the files with the contentMD5 between 0000000000000000 and 0000ffffffffffff.
Multiple bucket super ranges such as `VOSpac:000` which refers to the files in the first 16 buckets (0000 to 000f) or
`VOSpac:5` (buckets between `5000` and `5fff`) are also supported.

The AdStorageAdapter must authenticate to the AD TAP service for storage back end queries; this uses the cadcproxy.pem client certificate. 
The identity used must have permission to query the archive(s).
