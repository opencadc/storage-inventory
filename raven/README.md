# Storage Inventory locate service (raven)
This service implements transfer negotiation in a global inventory deployment.

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image docs 
for expected deployment and general config requirements.

### catalina.properties
When running raven.war in tomcat, parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:
```
# database connection pools
raven.invuser.maxActive={}
raven.invuser.username={}
raven.invuser.password={}
raven.invuser.url=jdbc:postgresql://{server}/{database}

```

### raven.properties
The following keys are required:
```
# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.raven.db.schema={schema}

org.opencadc.raven.publicKeyFile={public key file name}
org.opencadc.raven.privateKeyFile={private key file name}
```
The key file names are relative (no path) and the files must be in the config directory.

The following optional keys configure raven to use external service(s) to obtain grant information in order
to perform authorization checks:
```
org.opencadc.raven.readGrantProvider={resourceID of a permission granting service}
org.opencadc.raven.writeGrantProvider={resourceID of a permission granting service}
```
Multiple values of the permission granting service resourceID(s) may be provided by including multiple property 
settings. All services will be consulted but a single positive result is sufficient to grant permission for an 
action. TODO: `writeGrantProvider` and negotiation of write transfers are not yet implemented.

**For developer testing only:** To disable authorization checking (via `readGrantProvider` or `writeGrantProvider`
services), add the following configuration entry to raven.properties:
```
org.opencadc.raven.authenticateOnly=true
```
With `authenticateOnly=true`, any authenticated user will be able to read/write/delete files and anonymous users
will be able to read files.

## building

```
gradle clean build
docker build -t raven -f Dockerfile .
```

## checking it
```
docker run -it raven:latest /bin/bash
```

## running it
```
docker run --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name raven raven:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag raven:latest raven:$t
done
unset TAGS
docker image list raven
```
