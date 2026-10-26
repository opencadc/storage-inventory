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

# new
org.opencadc.inventory.storage.eos.EosStorageAdapter.subpath = {dir name under mgmServerPath}

# no longer used:
org.opencadc.inventory.storage.eos.EosStorageAdapter.artifactScheme = {scheme}

# database config for iterator used by tantar and eos-scanner
org.opencadc.eos.scan.schema={database schema for raw storage metadata dump}
org.opencadc.eos.scan.username={database username}
org.opencadc.eos.scan.password={database password}
org.opencadc.eos.scan.url=jdbc:postgresql://{server}/{database}
```
The `eos` database account owns and manages (create, alter, drop) database objects and modifies the content in the
specified `schema`. The database is specified in the JDBC URL. Failure to connect or initialize the database will show
up in logs and cause the application to exit. Note: the database config is only needed when running `eos-scanner` 
or `tantar` and is not required for `minoc`. The _schema_ MUST NOT be the same as the inventory schema.

The _mgmServer_ is a normal value of the EOS_MGM_URL environment variable used by eos-client tools; it is
normally of the form `root://{server name}`.

The _mgmServerPath_ selects the directory (in EOS) where files are stored. The EosStorageAdapter will 
generate `Artifact.uri` values using the configured _artifactScheme_ and the _relative_ path of files 
(relative to the configired _mgmServerPath_) as `{scheme}:{relative path}`.

The _mgmHttpsPort_ is used (with _mgmServer_ and _mgmServerPath_) to create https URLs to files for data
access.

The _authToken_ is used to authenticate to the EOS server.

The _artifactScheme_ is used to create Artifact.uri values from backend storage relative paths. This is used
by the `eos-scanner`.
