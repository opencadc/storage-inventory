# Storage Inventory VOSpace-2.1 service (vault)

The `vault` servcie is an implementation of the <a href="https://www.ivoa.net/documents/VOSpace/">IVOA VOSpace</a>
specification designed to co-exist with other storage-inventory components. It provides a heirarchical data
organization laye on top of the storage management of storage-inventory.

The simplest configuration would be to deploy `vault` with `minoc` with a single metadata database and single
back end storage system. Details: TBD.

The other option would be to deploy `vault` with `raven` and `luskan` in a global inventory database and make
use of one or more of the network of known storage sites to store files. Details: TBD.

## deployment

The `vault` war file can be renamed at deployment time in order to support an alternate service name, 
including introducing additional path elements. See 
<a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> (war-rename.conf).

## configuration
The following runtime configuration must be made available via the `/config` directory.

### catalina.properties
This file contains java system properties to configure the tomcat server and some of the java libraries used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a>
for common system properties.

`vault` includes multiple IdentityManager implementations to support authenticated access:
- See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.
- See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.

`vault` requires a connection pool to the local database:
```
# database connection pools
org.opencadc.vault.nodes.maxActive={max connections for vospace pool}
org.opencadc.vault.nodes.username={username for vospace pool}
org.opencadc.vault.nodes.password={password for vospace pool}
org.opencadc.vault.nodes.url=jdbc:postgresql://{server}/{database}
```
The _nodes_ account owns and manages (create, alter, drop) vospace database objects and manages
all the content (insert, update, delete). The database is specified in the JDBC URL and the schema name is specified 
in the vault.properties (below). Failure to connect or initialize the database will show up in logs and in the 
VOSI-availability output.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### vault.properties
A vault.properties file in /config is required to run this service.  The following keys are required:
```
# service identity
org.opencadc.vault.resourceID = ivo://{authority}/{name}

# vault database settings
org.opencadc.vault.inventory.schema = {inventory schema name}
org.opencadc.vault.vospace.schema = {vospace schema name}

# root container nodes
org.opencadc.vault.root.owner = {owner of root node}

# storage namespace
org.opencadc.vault.storage.namespace = {a storage inventory namespace to use}
```
The vault _resourceID_ is the resourceID of _this_ vault service.

The _inventory.schema_ name is the name of the database schema that contains the inventory database objects. The
account nominally requires read-only (select) permission on those objects. This currently must be "inventory" due
to configuration limitations in <a href="../luskan">luskan</a>.

The _vospace.schema_ name is the name of the database schema used for all created database objects (tables, indices, etc). Note that with a single connection pool, the two schemas must currently be in the same database.
TODO: augment config to support separate inventory and vospace pools.

The _root.owner_ owns the root node and has full read and write permission in the root container, so it can 
create and delete container nodes at the root and assign container node properties that are normally read-only
to normal users: owner, quota, etc. This must be set to the username of the admin.

The _storage.namespace_ configures `vault` to use the specified namespace in storage-inventory to store files. 
This only applies to new data nodes that are created and will not effect previously created nodes and artifacts.
Probably don't want to change this... prevent change? TBD.

### vault-availability.properties (optional)

The vault-availability.properties file specifies which users have the authority to change the availability state of 
the vault service. Each entry consists of a key=value pair. The key is always "users". The value is the x500 canonical 
user name.

Example:
```
users = {user identity}
```
`users` specifies the user(s) who are authorized to make calls to the service. The value is a list of user
identities (X500 distingushed name), one line per user. Optional: if the `vault-availability.properties` is 
not found or does not list any `users`, the service will function in the default mode (ReadWrite) and the 
state will not be changeable.

## building it
```
gradle clean build
docker build -t vault -f Dockerfile .
```

## checking it
```
docker run --rm -it vault:latest /bin/bash
```

## running it
```
docker run --rm --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name vault vault:latest
```

