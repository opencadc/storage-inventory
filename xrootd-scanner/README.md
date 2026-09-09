# Storage Inventory xrootd-scanner

This is an xrootd scanner that loads raw file metadata from xrootd into a database to support the
use of the EosStorageAdapter (cadc-storage-adapter-eos).

## configuration
Although this tool is not built on the standard [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) base image, the general config requirements should be the same.

Runtime configuration must be made available via the `/config` directory.

### xrootd-scanner.properties
```
org.opencadc.xrootd.logging = info|debug

# set whether to report all activity or to perform any actions required.
org.opencadc.xrootd.reportOnly = true|false

## database settings
org.opencadc.xrootd.username={database username}
org.opencadc.xrootd.password={database password}
org.opencadc.xrootd.url=jdbc:postgresql://{server}/{database}
```
The `xrootd` database account owns and manages (create, alter, drop) database objects and modifies the content. 
The database is specified in the JDBC URL. Failure to connect or initialize the database will show up in logs and cause the application to exit. Note: the database schema is obtained from the EosStorageAdapter configuration.

### cadc-storage-adapter-eos.properties
This tool is hard coded to use the EosStorageAdapter to read configuration.

## building it
```
gradle clean build
docker build -t xrootd-scanner -f Dockerfile .
```

## checking it
```
docker run -t xrootd-scanner:latest /bin/bash
```

## running it
```
docker run -r --user opencadc:opencadc -v /path/to/external/config:/config:ro --name xrootd-scanner xrootd-scanner:latest
```
