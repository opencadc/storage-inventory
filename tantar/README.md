# tantar v0.1

Process to ensure validity of the information stored in the Inventory Database and what is served from a Storage Adapter.  

This is intended to run periodically at any site to determine what changes, if any, are to be made to either the Storage Inventory database, or
the storage system.

## Configuration

A file called `tantar.properties` should be available from the `/config` directory.  All properties in the file are loaded and set
as System properties.

### Template configuration (`tantar.properties`)

```
#######
## Common settings
#######
org.opencadc.inventory.logging = INFO
org.opencadc.inventory.{name}.logging = INFO

#######
## Validator settings
#######
# Used to set the bucket to tantar
org.opencadc.tantar.bucket = {bucketname}

#######
## Storage Inventory settings
#######
# The SQL generator implementation (default shown)
#org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator

# The database settings for the Inventory database.
org.opencadc.inventory.db.schema={schema}
org.opencadc.inventory.db.username={dbuser}
org.opencadc.inventory.db.password={dbpassword}
org.opencadc.inventory.db.url={jdbcurl}

#######
## Storage Adapter specific settings
#######
# The storage adapter to use for storage.
# Consult the storage adapter instructions for adapter-specific configuration.  The library MUST be
# accessible from this application's classpath.
#org.opencadc.inventory.storage.StorageAdapter={fully-qualified-classname of implementation}
```


## Building it
This Docker image relies on the [Base Java Docker image](https://github.com/opencadc/docker-base/tree/master/cadc-java) built as an image called `cadc-java`.

```
gradle -i clean build
docker build -t opencadc/tantar -f Dockerfile .
```

## Checking it
```
docker run -t opencadc/tantar:latest
```

## Running it
Running as the `nobody` user is recommended:
```
docker run -r --user nobody:nobody -v /path/to/external/config:/config:ro --name tantar opencadc/tantar:latest
```

Or as root:
```
docker run -r -v /path/to/external/config:/config:ro --name tantar opencadc/tantar:latest
```
