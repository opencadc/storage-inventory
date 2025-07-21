# cadc-storage-adapter-eos
EOS storage adapter implementation

### cadc-storage-adapter-eos.properties

The following StorageAdapter implementations are provided:

|fully qualified class name|description|
|--------------------------|-----------|
|org.opencadc.inventory.storage.eos.EosStorageAdapter|read-only EOS adapter|

Example:
```
org.opencadc.inventory.storage.eos.EosStorageAdapter.mgmServer = {the usual EOS_MGM_URL environment value}
org.opencadc.inventory.storage.eos.EosStorageAdapter.mgmServerPath = {path in EOS server to ingest}
org.opencadc.inventory.storage.eos.EosStorageAdapter.mgmHttpsPort = {https port}

org.opencadc.inventory.storage.eos.EosStorageAdapter.authToken = zteos64:{token}

org.opencadc.inventory.storage.eos.EosStorageAdapter.artifactScheme = {scheme}
```

The _mgmServer_ is a normal value of the EOS_MGM_URL environment variable used by eos-client tools; it is
normally of the form `root://{server name}`.

The _mgmServerPath_ selects the directory (in EOS) where files are stored. The EosStorageAdapter will 
generate `Artifact.uri` values using the configured _artifactScheme_ and the _relative_ path of files 
(relative to the configired _mgmServerPath_) as `{scheme}:{relative path}`.

The _mgmHttpsPort_ is used (with _mgmServer_ and _mgmServerPath_) to create https URLs to files for data
access.

The _authToken_ is used to authenticate to the EOS server.



