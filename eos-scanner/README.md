# Storage Inventory xrootd-scanner

This is an xrootd scanner that loads raw file metadata from EOS into a database to support the
use of the EosStorageAdapter (cadc-storage-adapter-eos).

## configuration
Although this tool is not built on the standard [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) base image, the general config requirements should be the same.

Runtime configuration must be made available via the `/config` directory.

### eos-scanner.properties
```
org.opencadc.eoscan.logging = info|debug

# set whether to report all activity or to perform any actions required.
org.opencadc.eoscan.reportOnly = true|false

## database settings
org.opencadc.eoscan.schema={database schema for raw storage metadata dump}
org.opencadc.eoscan.username={database username}
org.opencadc.eoscan.password={database password}
org.opencadc.eoscan.url=jdbc:postgresql://{server}/{database}

org.opencadc.eoscan.artifactScheme = {scheme}
```
The `eoscan` database account owns and manages (create, alter, drop) database objects and modifies the content in the
specified `schema`. The database is specified in the JDBC URL. Failure to connect or initialize the database will show
up in logs and cause the application to exit. Note: the database schema is obtained from the EosStorageAdapter configuration.

The **database and the database account must** be the same one used by `tantar` (the inventory admin account).

The `artifactScheme` 
### cadc-storage-adapter-eos.properties
This tool is hard coded to use the EosStorageAdapter to read EOS connection configuration.

## building it
```
gradle clean build
docker build -t eos-scanner -f Dockerfile .
```

## checking it
```
docker run -t eos-scanner:latest /bin/bash
```

## running it
```
docker run -r --user opencadc:opencadc -v /path/to/external/config:/config:ro --name eos-scanner eos-scanner:latest
```
