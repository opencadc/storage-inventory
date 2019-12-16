# Artifact storage and management service (minoc)

### building

1. gradle clean build
2. docker build -t minoc -f Dockerfile .

### checking it
docker run --rm -it minoc:latest /bin/bash

### running it on container port 8080
docker run --rm -d -v /<myconfigdiar>:/conf:ro -v /<mylogdir>:/logs:rw minoc:latest

### running it on localhost port 80
docker run --rm -d -v /<myconfigdir>:/conf:ro -v /<mylogdir>:/logs:rw -p 80:8080 minoc:latest

### minoc.properties
A minoc.properties file in /<myconfigdir> is required to run this service.  The following keys (with example values) are needed:

\# The storage adapter to use for storage.

org.opencadc.inventory.storage.StorageAdapter=org.opencadc.inventory.storage.fs.FileSystemStorageAdapter

\# The SQL generator implementation

org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator

\# The database to use

org.opencadc.inventory.db.database=content

\# The schema to use

org.opencadc.inventory.db.schema=inventory

### catalina.properties
When running minoc.war in tomcat, some parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:

\# The maximum number of active database connections

minoc.invadm.maxActive=1

\# The username with which to connect

minoc.invadm.username=invadm

\# The password for the username

minoc.invadm.password=pw-invadm

\# The JDBC connection URL

minoc.invadm.url=jdbc:postgresql://mydbhost/content
