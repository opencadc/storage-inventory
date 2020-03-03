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

# service identity
org.opencadc.minoc.resourceID=ivo://{authority}/{name}
```
Additional java system properties may be required by the storage adapter implementation.

### LocalAuthority.properties
The LocalAuthority.properties file specifies which local service is authoritative for various site-wide functions. The keys
are standardID values for the functions and the values are resouceID values for the service that implements that standard 
feature.

Example:
```
ivo://ivoa.net/std/GMS#search-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#users-0.1 = ivo://cadc.nrc.ca/gms    
ivo://ivoa.net/std/UMS#login-0.1 = ivo://cadc.nrc.ca/gms           

ivo://ivoa.net/std/CDP#delegate-1.0 = ivo://cadc.nrc.ca/cred
ivo://ivoa.net/std/CDP#proxy-1.0 = ivo://cadc.nrc.ca/cred
```

### minoc.properties
A minoc.properties file in /config is required to run this service.  The following keys (with example values) are needed:

```
# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.inventory.db.schema={schema}

# permission granting service settings
org.opencadc.inventory.permissions.ReadGrant.resourceID=ivo://{authority}/{name}
org.opencadc.inventory.permissions.WriteGrant.resourceID=ivo://{authority}/{name}
```
Multiple values of the permission granting service resourceID(s) may be provided. All services will be consulted but a single
positive result is sufficient to grant permission for an action.

Additional configuration may be required by the storage adapter implementation.

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
Note: If you use cadc-storage-adapter-fs you probably also want to volume mount an external directory 
read-write to store files.

