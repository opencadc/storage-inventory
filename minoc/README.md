# Storage Inventory file service (minoc)

## building

```
gradle clean build
docker build -t minoc -f Dockerfile .
```

## checking it
```
docker run -it minoc:latest /bin/bash
```

## running it
Note: if using the cadc-storage-adapter-fs implementation, the file system root needs to be mapped to an external volume in the docker run command described below. 
```
docker run -d --volume=/path/to/external/config:/config:ro --volume=/path/to/external/logs:/logs:rw --name minoc minoc:latest
```

## configuration
See the <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> image docs 
for expected deployment and general config requirements.

### catalina.properties
When running minoc.war in tomcat, parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:
```
# The maximum number of active database connections
minoc.invadm.maxActive=1

# The username with which to connect
minoc.invadm.username=invadm

# The password for the username
minoc.invadm.password=pw-invadm

# The JDBC connection URL
minoc.invadm.url=jdbc:postgresql://mydbhost/content
```

### minoc.properties
A minoc.properties file in /config is required to run this service.  The following keys (with example values) are needed:

```
# The storage adapter to use for storage.
# Consult the storage adapter instructions for adapter-specific configuration
org.opencadc.inventory.storage.StorageAdapter=org.opencadc.inventory.storage.fs.FileSystemStorageAdapter

# The SQL generator implementation (optional property, will default to the once below if not present)
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator

# The schema to use
org.opencadc.inventory.db.schema=inventory

# The service ID of a permissions service(s) providing read permissions. There may be multiple instances of this key/value pair.
org.opencadc.inventory.permissions.ReadGrant.serviceID=ivo://cadc.nrc.ca/servicewithperms

# The service ID of a permissions system providing write pwermissions. There may be multiple instances of this key/value pair.
org.opencadc.inventory.permissions.WriteGrant.serviceID=ivo://cadc.nrc.ca/servicewithperms
```
