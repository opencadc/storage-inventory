# Storage Inventory file service (minoc)

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs 
for expected deployment and general config requirements.

### catalina.properties
When running minoc.war in tomcat, parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:
```
# database connection pools
minoc.invadm.maxActive={}
minoc.invadm.username={}
minoc.invadm.password={}
minoc.invadm.url=jdbc:postgresql://{server}/{database}
```
Additional java system properties may be required by libraries.

### minoc.properties
A minoc.properties file in /config is required to run this service.  The following keys are required:
```
# service identity
org.opencadc.minoc.resourceID=ivo://{authority}/{name}

# storage back end
org.opencadc.inventory.storage.StorageAdapter={fully qualified classname of StorageAdapter impl}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.minoc.db.schema={schema}
```
The following optional keys configure minoc to use external service(s) to obtain grant information in order
to perform authorization checks:
```
# permission granting services (optional)
org.opencadc.minoc.readGrantProvider={resourceID of a permission granting service}
org.opencadc.minoc.writeGrantProvider={resourceID of a permission granting service}
```
Multiple values of the permission granting service resourceID(s) may be provided by including multiple property 
settings. All services will be consulted but a single positive result is sufficient to grant permission for an 
action.

**For developer testing only:** To disable authorization checking (via `readGrantProvider` or `writeGrantProvider`
services), add the following configuration entry to minoc.properties:
```
org.opencadc.minoc.authenticateOnly=true
```
With `authenticateOnly=true`, any authenticated user will be able to read/write/delete files and anonymous users
will be able to read files.

Additional configuration may be required by the storage adapter implementation.

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
```

## building it
```
gradle clean build
docker build -t minoc -f Dockerfile .
```

## checking it
```
docker run -it minoc:latest /bin/bash
```

## running it
```
docker run --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name minoc minoc:latest
```

