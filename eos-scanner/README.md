# Storage Inventory eos-scanner

This is an EOS scanner that loads raw file metadata from EOS into a database to support the
use of the EosStorageAdapter (cadc-storage-adapter-eos).

## configuration
Although this tool is not built on the standard [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) base image, the general config requirements should be the same.

Runtime configuration must be made available via the `/config` directory.

### eos-scanner.properties
```
org.opencadc.eos.logging = info|debug

org.opencadc.eos.dirScanOnly = true|false 

org.opencadc.eos.subdir = {subdir under mgmServerPath to scan}

## database settings: see cadc-storage-adapter-eos.properties
```

### cadc-storage-adapter-eos.properties
This tool is hard coded to use the EosStorageAdapter to read EOS and database connection configuration.

## work directory
This tool writes the result of the directory scan to `/work/{subdir}.txt` and will reuse the file to perform
the rest of the scanning. The operator can mount an external volume here if it wants to examine the dir scan
results and/or keep them across different runs.

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
