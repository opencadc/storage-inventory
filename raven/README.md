# Storage Inventory locate service (raven)
This service implements transfer negotiation in a global inventory deployment.

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image docs 
for expected deployment and general config requirements.

Runtime configuration must be made available via the `/config` directory.

### catalina.properties
When running raven.war in tomcat, parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:
```
# database connection pools
org.opencadc.raven.query.maxActive={max connections for query pool}
org.opencadc.raven.query.username={database username for query pool}
org.opencadc.raven.query.password={database password for query pool}
org.opencadc.raven.query.url=jdbc:postgresql://{server}/{database}

```
The `query` pool is used to query inventory for the requested Artifact.

### raven.properties
The following keys are required:
```
# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.raven.inventory.schema={schema}

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
action.

Another set of optional keys configure raven to prioritize sites returned in a transfer, with higher priority
sites first in the list of transfer URL's to GET or PUT a file.
```
org.opencadc.raven.putPreference={storage site name}
{storage site name).resourceID={storage site resourceID}
{storage site name).namespace={storage site namespace}
```

**For developer testing only:** To disable authorization checking (via `readGrantProvider` or `writeGrantProvider`
services), add the following configuration entry to raven.properties:
```
org.opencadc.raven.authenticateOnly=true
```
With `authenticateOnly=true`, any authenticated user will be able to read/write/delete files and anonymous users
will be able to read files.

### LocalAuthority.properties
The LocalAuthority.properties file specifies which local service is authoritative for various site-wide functions. The keys
are standardID values for the functions and the values are resourceID values for the service that implements that standard 
feature.

Example:
```
ivo://ivoa.net/std/GMS#search-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#users-0.1 = ivo://cadc.nrc.ca/gms    
ivo://ivoa.net/std/UMS#login-0.1 = ivo://cadc.nrc.ca/gms           

ivo://ivoa.net/std/CDP#delegate-1.0 = ivo://cadc.nrc.ca/cred
ivo://ivoa.net/std/CDP#proxy-1.0 = ivo://cadc.nrc.ca/cred

org.opencadc.raven.putPreference=@CADC
@CADC.resourceID=ivo://cadc.nrc.ca/cadc/minoc
@CADC.namespace=cadc:IRIS/
@CADC.namespace=cadc:CGPS/
```

### cadcproxy.pem
This client certificate is used to make server-to-server calls for system-level A&A purposes.

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
