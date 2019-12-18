# Artifact locate service (raven)

### building

1. gradle clean build
2. docker build -t raven -f Dockerfile .

### checking it
docker run --rm -it raven /bin/bash

### running it on container port 8080
docker run --rm -d -v /<myconfigdiar>:/conf:ro -v /<mylogdir>:/logs:rw raven:latest

### running it on localhost port 80
docker run --rm -d -v /<myconfigdir>:/conf:ro -v /<mylogdir>:/logs:rw -p 80:8080 raven:latest

### minoc.properties
A raven.properties file in /<myconfigdir> is required to run this service.  The following keys (with example values) are needed:

```
# The storage adapter to use for storage.
org.opencadc.inventory.storage.StorageAdapter=org.opencadc.inventory.storage.fs.FileSystemStorageAdapter

# The SQL generator implementation (optional property, will default to the once below if not present)
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator

# The schema to use
org.opencadc.inventory.db.schema=inventory

# The service ID of a permissions system providing read permission grants.  There may be multiple instances of this key/value pair.
org.opencadc.inventory.permissions.ReadGrant.serviceID=ivo://cadc.nrc.ca/servicewithperms

# The service ID of a permissions system providing write permission grants.  There may be multiple instances of this key/value pair.
org.opencadc.inventory.permissions.WriteGrant.serviceID=ivo://cadc.nrc.ca/servicewithperms
```

### catalina.properties
When running raven.war in tomcat, some parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:

```
# The maximum number of active database connections
raven.invadm.maxActive=1

# The username with which to connect
raven.invadm.username=invadmr

# The password for the username
raven.invadm.password=pw-invadm

# The JDBC connection URL
raven.invadm.url=jdbc:postgresql://mydbhost/content
```
