# Storage Inventory xrootd-scanner

This is an xrootd scanner that loads raw file metadata from EOS into a database to support the
use of the EosStorageAdapter (cadc-storage-adapter-eos).

## configuration
Although this tool is not built on the standard [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) base image, the general config requirements should be the same.

Runtime configuration must be made available via the `/config` directory.

### eos-scanner.properties
```
org.opencadc.eos.logging = info|debug

# set whether to report all activity or to perform any actions required.
org.opencadc.eos.reportOnly = true|false

## database settings: see cadc-storage-adapter-eos.properties

```

The **database and the database account must** be the same one used by `tantar` (the inventory admin account).

### cadc-storage-adapter-eos.properties
This tool is hard coded to use the EosStorageAdapter to read EOS and database connection configuration.

## building it
```
gradle clean build
docker build -t eos-scanner -f Dockerfile .
```

## checking it
```
docker run -it --rm eos-scanner:latest /bin/bash
```

## running it
```
docker run --rm --user opencadc:opencadc -v /path/to/external/config:/config:ro --name eos-scanner eos-scanner:latest
```
