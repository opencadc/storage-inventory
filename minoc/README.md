# Storage Inventory file service (minoc)

This is an implementation of the **file** service that supports HEAD, GET, PUT, POST, DELETE operations
and <a href="https://www.ivoa.net/documents/SODA/">IVOA SODA</a> operations.

### deployment
The `minoc` war file can be renamed at deployment time in order to support an alternate service name, including introducing 
additional path elements (see war-rename.conf).

### configuration

The following runtime configuration must be made available via the `/config` directory.

### catalina.properties
This file contains java system properties to configure the tomcat server and some of the java libraries used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a>
for common system properties.

`minoc` includes multiple IdentityManager implementations to support authenticated access:
- See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.
- See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.

`minoc` requires a connection pool to the local storage inventory database:
```
# database connection pools
org.opencadc.minoc.inventory.maxActive={max connections for inventory admin pool}
org.opencadc.minoc.inventory.username={username for inventory admin pool}
org.opencadc.minoc.inventory.password={password for inventory admin pool}
org.opencadc.minoc.inventory.url=jdbc:postgresql://{server}/{database}
```
The `inventory` account owns and manages (create, alter, drop) inventory database objects and manages
all the content (insert, update, delete). The database is specified in the JDBC URL and the schema name is specified 
in the minoc.properties (below). Failure to connect or initialize the database will show up in logs and in the 
VOSI-availability output.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### minoc.properties
A minoc.properties file in /config is required to run this service.  The following keys are required:
```
# service identity
org.opencadc.minoc.resourceID=ivo://{authority}/{name}

# storage back end
org.opencadc.inventory.storage.StorageAdapter={fully qualified classname of StorageAdapter impl}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.minoc.inventory.schema={schema name in the database configured in the JDBC URL}
```
The minoc _resourceID_ is the resourceID of _this_ minoc service.

The _StorageAdapter_ is a plugin implementation to support the back end storage system. These are implemented in separate libraries;
each available implementation is in a library named _cadc-storage-adapter-{*impl*}_ and the fully qualified class name to use is 
documented there. Additional java system properties and/or configuration files may be required to configure the appropriate storage adapter:
- [Swift Storage Adapter](https://github.com/opencadc/storage-inventory/tree/master/cadc-storage-adapter-swift)

- [File System Storage Adapter](https://github.com/opencadc/storage-inventory/tree/master/cadc-storage-adapter-fs)

- [AD Storage Adapter](https://github.com/opencadc/storage-inventory/tree/master/cadc-storage-adapter-ad)


The _SQLGenerator_ is a plugin implementation to support the database. There is currently only one implementation that is tested 
with PostgeSQL (10+). Making this work  with other database servers in future _may_ require a different implementation.

The inventory _schema_ name is the name of the database schema used for all created database objects (tables, indices, etc). This
currently must be "inventory" due to configuration limitations in <a href="../luskan">luskan</a>.

The following keys are optional:
```
# services that are trusted to generate pre-auth URLs
org.opencadc.minoc.trust.preauth = {resourceID}

# operations permissions (optional)
org.opencadc.minoc.archiveOperator = {X509 distinguished name}
org.opencadc.minoc.fileSyncOperator = {X509 distinguished name}

# permission granting services (optional)
org.opencadc.minoc.readGrantProvider={resourceID of a permission granting service}
org.opencadc.minoc.writeGrantProvider={resourceID of a permission granting service}

# override the implied readable and writable state
org.opencadc.minoc.readable = true|false
org.opencadc.minoc.writable = true|false

# configure StorageAdapter delete behaviour
org.opencadc.minoc.recoverableNamespace = {namespace}
```
The optional _trust.preauth_ key(s) configure `minoc` to trust external service(s) to have performed
authorization checks. Such services may include a signed token in the URL and `minoc` will validate 
the request using a public key retrieved from the service instead of performing authorization checks
itself. Currently, only `raven` and `vault` can generate such URLs and provide access to their
public keys. Example:
```
# trust a SI global inventory
org.opencadc.minoc.trust.preauth = ivo://example.net/raven

# trust a SI VOSpace service
org.opencadc.minoc.trust.preauth = ivo://example.net/vault
```
Setting _trust.preauth_ one or more times implicitly sets _readable_ and _writable_ to _true_.

The optional _archiveOperator_ and _fileSyncOperator_ keys grant permissions directly to operational
users. An _archiveOperator_ has read-write permission to **ALL** namespaces and files and can be used 
to enable processes to put files into the archive. A _fileSyncOperator_ has read-only permission to
**ALL** namespaces and files and can be used to enable file-sync (`critwall` running at a different 
storage site). Both of these can be used with no additional AAI components, but they do require front-end
proxy setup and an IdentityManager implementation to support X509 client certificate use.

The optional _readGrantProvider_ and _writeGrantProvider_ keys configure minoc to call other services to 
get grants (permissions) for operations. Multiple values of the granting service resourceID(s) may be provided 
by including multiple property settings (one per line). All services will be consulted but a single positive 
result is sufficient to grant permission for an action. Setting these values implicitly sets _readable_
and _writable_ to _true_ respectively.

The optional _readable_ and _writable_ keys configure minoc explicitly rather than relying on one or more of 
the above trust or grant provider settings. For example, this allows one to configure a minoc service to
advertise itself as read-only (_writable_ = false) even though local authorisation would allow read-write
access.

The optional _recoverableNamespace_ key causes `minoc` to configure the storage adapter so that deletions
preserve the file content in a recoverable state. This generally means that storage space remains in use
but details are internal to the StorageAdapter implementation . Multiple values may be provided by 
including multiple property settings in order to make multiple namespace(s) recoverable. The namespace
value(s) must end with a colon (:) or slash (/) so one namespace cannot _accidentally_ match (be a prefix of) 
another namespace.
Example:
```
org.opencadc.minoc.recoverableNamespace = cadc:
org.opencadc.minoc.recoverableNamespace = test:KEEP/

# redundant but OK
org.opencadc.minoc.recoverableNamespace = cadc:IMPORTANT/
```
Artifacts that are deleted via the `minoc` API with an `Artifact.uri` that matches (starts with) one of these 
prefixes will be recoverable. Others (e.g. `test:FOO/bar`) will be permanently deleted and not recoverable.

Note: Since artifact and stored object deletion can also be performed by the `tantar` file validation tool,
all instances of `minoc` and `tantar` that use the same inventory and storage adapter should use the same
 _recoverableNamespace_ configuration so that preservation and recovery (from mistakes) is consistent.
---
**For developer testing only:** To disable authorization checking (via `readGrantProvider` or `writeGrantProvider`
services), add the following configuration entry to minoc.properties:
```
org.opencadc.minoc.authenticateOnly=true
```
With `authenticateOnly=true`, any authenticated user will be able to read/write/delete files and anonymous users
will be able to read files.

### cadc-log.properties (optional)
See <a href="https://github.com/opencadc/core/tree/master/cadc-log">cadc-log</a> for common 
dynamic logging control.

### cadc-vosi.properties (optional)
See <a href="https://github.com/opencadc/reg/tree/master/cadc-vosi">cadc-vosi</a> for common 
service state control.

### cadcproxy.pem (optional)
This client certificate is used to make authenticated server-to-server calls for system-level A&A purposes.

## building it
```
gradle clean build
docker build -t minoc -f Dockerfile .
```

## checking it
```
docker run --rm -it minoc:latest /bin/bash
```

## running it
```
docker run --rm --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name minoc minoc:latest
```

