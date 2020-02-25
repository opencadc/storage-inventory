# cadc-file-validate 0.1

Command line tool to ensure validity of the information stored in the Inventory Database and what is served from a Storage Adapter.  

This is intended to run periodically at any site to determine what changes, if any, are to be made to either the Storage Inventory database, or
the storage system.

## Configuration

A file called `cadc-file-validate.properties` should be available from the `/config` directory.  All properties in the file are loaded and set
as System properties:

```java
Properties properties = new Properties();
properties.load("/config/cadc-file-validate.properties");
System.setProperties(properties);
```

### Sample configuration (`cadc-file-validate.properties`)

```
# Used to set the bucket range to validate
org.opencadc.inventory.validate.bucket.start = {bucket-range-start}
org.opencadc.inventory.validate.bucket.end = {bucket-range-end}

#######
## Storage Adapter Inventory settings
#######

# The storage adapter to use for storage.
# Consult the storage adapter instructions for adapter-specific configuration.  The library MUST be
# accessible from this application's classpath.
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


## Building it
```
gradle -i clean build
docker build -t opencadc/cadc-file-validate -f Dockerfile .
```

## Checking it
```
docker run -t opencadc/cadc-file-validate:latest
```

### With usage
```
docker run -t opencadc/cadc-file-validate:latest -h
```


## Running it
```
docker run -r -v /path/to/external/config:/config:ro --name validate opencadc/cadc-file-validate:latest
```
