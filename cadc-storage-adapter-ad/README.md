# cadc-storage-adapter-ad
AD storage adapter implementation. 

Currently supports GET and Iterator functionality only. 
Requires credentials valid for calling the ad web service
be in the Subject. Credentials must be provided by the calling program.

This library currently provides a partial implementation of the StorageAdapter plugin API:

_org.opencadc.inventory.storage.ad.AdStorageAdapter_ 
supports get and iterator (streaming list) so can be used in a read-only minoc instance and a tantar instance with the 
_StorageIsAlwaysRight_ policy. A bucket with this adapter defined by the name of an archive in AD. The adapter also supports
sub-buckets that are based on the first 1 to 3 digits of the files MD5 checksum, such as: "JCMT:1" (all the files in the JCMT
bucket with the MD5 checksum starting in 1 - 16th of the entire JCMT bucket), "CFHT:ab" (256th of the CFHT bucket), 
"VOSpac:fff" (4096th of the VOSpac bucket), etc.

The AdStorageAdapter must authenticate to the AD TAP service for storage back end queries; this uses the cadcproxy.pem client certificate. 
The identity used must have permission to query the archive(s).
