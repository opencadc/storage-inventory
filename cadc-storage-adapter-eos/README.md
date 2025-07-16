# cadc-storage-adapter-eos
EOS storage adapter implementation

### cadc-storage-adapter-eos.properties

The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.eos.EosStorageAdapter|read-only EOS adapter|

Example:
```
org.opencadc.inventory.storage.eos.EosStorageAdapter.mgmServer = {base HTTPS URL of EOS MGM server}
org.opencadc.inventory.storage.eos.EosStorageAdapter.authToken = zteos64:{token}
```

The EosStorageAdapter will generate `Artifact.uri` values from the path(s) to files in the MGM< server
_relative_ to the base URL: the base URL must include all path elements that should be **ignored** when
iterating and constructing logical identifiers from the remaining relative path(s). Details: TBD.




