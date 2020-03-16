# Storage Inventory file-sync process (critwall)

Process to incrementally sync metadata changes between storage sites and global inventory(ies). This process
is a hydrid multiprocess-multithread model: the operator runs one process per Artifact.uriBucket and multiple
download threads within that process.

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

A file called `critwall.properties` must be made available via the `/config` directory.

### critwall.properties
```
org.opencadc.critwall.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.critwall.db.schema={schema}
org.opencadc.critwall.db.username={dbuser}
org.opencadc.critwall.db.password={dbpassword}
org.opencadc.critwall.db.url=jdbc:postgresql://{server}/{database}

# global transfer negotiation service (raven)
org.opencadc.critwall.locatorService={resorceID of global transfer negotiation service}

# storage back end
org.opencadc.inventory.storage.StorageAdapter=org.opencadc.inventory.storage.fs.FileSystemStorageAdapter

# file-sync
org.opencadc.critwall.buckets = {uriBucket prefix}
org.opencadc.critwall.threads = {number of download threads}
```

### cadcproxy.pem
Querying the global locator service (raven) and downloading files (minoc) requires permission. `critwall` uses 
this certificate file located in /config to authenticate.

## building it
```
gradle clean build
docker build -t critwall -f Dockerfile .
```

## checking it
```
docker run -it critwall:latest /bin/bash
```

## running it
```
docker run --user nobody:nobody -v /path/to/external/config:/config:ro --name critwall critwall:latest
```

