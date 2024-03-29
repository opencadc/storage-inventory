# Storage Inventory storage management service (vault)

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image docs
for expected deployment and general config requirements. The `vault` war file can be renamed
at deployment time in order to support an alternate service name, including introducing 
additional path elements (see war-rename.conf).

Runtime configuration must be made available via the `/config` directory.

### catalina.properties
When running vault.war in tomcat, parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:
```
# database connection pools
org.opencadc.vault.nodes.maxActive={max connections for vospace pool}
org.opencadc.vault.nodes.username={username for vospace pool}
org.opencadc.vault.nodes.password={password for vospace pool}
org.opencadc.vault.nodes.url=jdbc:postgresql://{server}/{database}
```
The `nodes` account owns and manages (create, alter, drop) vault database objects and manages
all the content (insert, update, delete). The database is specified in the JDBC URL and the schema name is specified 
in the vault.properties (below). Failure to connect or initialize the database will show up in logs and in the 
VOSI-availability output.

### vault.properties
A vault.properties file in /config is required to run this service.  The following keys are required:
```
# service identity
org.opencadc.vault.resourceID=ivo://{authority}/{name}

# vault database settings
org.opencadc.vault.nodes.schema={schema name}
```
The vault _resourceID_ is the resourceID of _this_ vault service.

The nodes _schema_ name is the name of the database schema used for all created database objects (tables, indices, etc).

### vault-availability.properties (optional)
```
The vault-availability.properties file specifies which users have the authority to change the availability state of the vault service. Each entry consists of a key=value pair. The key is always "users". The value is the x500 canonical user name.
```

Example:
```
users = {user identity}
```
`users` specifies the user(s) who are authorized to make calls to the service. The value is a list of user identities (X500 distingushed name), one line per user. Optional: if the `vault-availability.properties` is not found or does not list any `users`, the service will function in the default mode (ReadWrite) and the state will not be changeable.

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

## apply semantic version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag vault:latest vault:$t
done
unset TAGS
docker image list vault
```
