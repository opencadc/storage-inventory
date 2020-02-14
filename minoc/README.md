# Storage Inventory file service (minoc)

## configuration
See the <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> image docs 
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

### LocalAuthority.properties
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
# The storage adapter to use for storage.
# Consult the storage adapter instructions for adapter-specific configuration
#org.opencadc.inventory.storage.StorageAdapter={fully-qualified-classname of implementation}
org.opencadc.inventory.storage.StorageAdapter=org.opencadc.inventory.storage.fs.FileSystemStorageAdapter

# The SQL generator implementation (default shown)
#org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator

# The schema to use
org.opencadc.inventory.db.schema=inventory

# The identity of a permissions service(s) providing read permissions. 
# - multiple values are supported: service will query all of them
#org.opencadc.inventory.permissions.ReadGrant.resource=ivo://{authority}/{name}

# The identity of a permissions system providing write permissions. 
# - multiple values are supported: service will query all of them 
#org.opencadc.inventory.permissions.WriteGrant.resourceID=ivo://{authority}/{name}
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
Note: If you use cadc-storage-adapter-fs you probably also want to volume mount an external directory 
read-write to store files.

