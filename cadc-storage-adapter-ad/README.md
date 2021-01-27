# cadc-storage-adapter-ad
AD storage adapter implementation. 

Currently supports GET and Iterator functionality only. 
Requires credentials valid for calling the ad web service
be in the Subject. Credentials must be provided by the calling program.

This library currently provides a partial implementation of the StorageAdapter plugin API:

_org.opencadc.inventory.storage.ad.AdStorageAdapter_ 
supports get and iterator (streaming list) so can be used in a read-only minoc instance and a tantar instance with the 
_StorageIsAlwaysRight_ policy.
