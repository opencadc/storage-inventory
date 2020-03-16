# Storage Inventoty metadata-sync process (fenwick)

Process to incrementally sync metadata changes between storage sites and global inventory(ies).

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

A file called `fenwick.properties` must be made available via the `/config` directory.

### fenwick.properties
```
org.opencadc.fenwick.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.inventory.db.schema={schema}
org.opencadc.fenwick.username={dbuser}
org.opencadc.fenwick.password={dbpassword}
org.opencadc.fenwick.url=jdbc:postgresql://{server}/{database}

# remote inventory query service (luskan)
org.opencadc.fenwick.queryService={resorceID of remote TAP service with inventory data model}

# selectivity
org.opencadc.fenwick.ArtifactSelector={fully qualified classname of ArtifactSelector implementation}
```

### cadcproxy.pem
Querying the remote query service (luskan) requires permission. `fenwick` uses this certificate file located
in /config to authenticate.

## building it
```
gradle clean build
docker build -t fenwick -f Dockerfile .
```

## checking it
```
docker run -it fenwick:latest /bin/bash
```

## running it
```
docker run --user nobody:nobody -v /path/to/external/config:/config:ro --name fenwick fenwick:latest
```

