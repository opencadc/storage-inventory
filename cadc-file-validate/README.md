# cadc-file-validate 0.1

Process to ensure validity of the information stored in the Inventory Database and what is served from a Storage Adapter.  

This is intended to run periodically at any site to determine what changes, if any, are to be made to either the Storage Inventory database, or
the storage system.

## Configuration

A file called `cadc-file-validate.properties` should be available from the `/config` directory.  All properties in the file are loaded and set
as System properties.

### Sample configuration (`cadc-file-validate.properties`)

```
#######
## Common settings
#######
org.opencadc.inventory.logging = INFO
org.opencadc.inventory.validate.logging = INFO

#######
## Validator settings
#######
# Used to set the bucket range to validate
org.opencadc.inventory.validate.bucket.start = {bucket-range-start}
org.opencadc.inventory.validate.bucket.end = {bucket-range-end}

#######
## Storage Inventory settings
#######
# The SQL generator implementation (default shown)
#org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator

# The database settings for the Inventory database.
org.opencadc.inventory.db.schema=inventory
org.opencadc.inventory.db.username=pguser
org.opencadc.inventory.db.password=pgpass
org.opencadc.inventory.db.url=jdbc:postgresql://myhost/mydb

#######
## Storage Adapter specific settings
#######
# The storage adapter to use for storage.
# Consult the storage adapter instructions for adapter-specific configuration.  The library MUST be
# accessible from this application's classpath.
#org.opencadc.inventory.storage.StorageAdapter={fully-qualified-classname of implementation}
org.opencadc.inventory.storage.StorageAdapter=org.opencadc.inventory.storage.fs.FileSystemStorageAdapter
```


## Building it
This Docker image relies on the [Base Java Docker image](https://github.com/opencadc/docker-base/tree/master/cadc-java) built as an image called `cadc-java`.

```
gradle -i clean build
docker build -t opencadc/cadc-file-validate -f Dockerfile .
```

## Checking it
```
docker run -t opencadc/cadc-file-validate:latest
```

## Running it
```
docker run -r -v /path/to/external/config:/config:ro --name validate opencadc/cadc-file-validate:latest
```
