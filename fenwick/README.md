# Storage Inventoty metadata-sync process (fenwick)

Process to sync metadata changes between storage sites and global inventory(ies). This process runs in
incremental mode (single process running continuously to update a local inventory database). 

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

A file called `fenwick.properties` must be made available via the `/config` directory.

### fenwick.properties
```
org.opencadc.fenwick.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.fenwick.db.schema={schema}
org.opencadc.fenwick.db.username={dbuser}
org.opencadc.fenwick.db.password={dbpassword}
org.opencadc.fenwick.db.url=jdbc:postgresql://{server}/{database}

# Enable Storage Site lookup to synchronize site locations on Artifacts
org.opencadc.fenwick.trackSiteLocations={true|false}

# remote inventory query service (luskan)
org.opencadc.fenwick.queryService={resorceID of remote TAP service with inventory data model}

# selectivity
org.opencadc.fenwick.ArtifactSelector={fully qualified classname of ArtifactSelector implementation}
```

### cadcproxy.pem
Querying the remote query service (luskan) requires permission. `fenwick` uses this certificate file located
in /config to authenticate.

### SQL includes
When the `org.opencadc.fenwick.IncludeArtifacts` ArtifactSelector is specified, an SQL file specifying the included Artifacts is required 
in the `/config/include` folder.  The single clause in the SQL file *MUST* begin with the `WHERE` keyword.

> `artifact-include.sql`
```sql
WHERE uri LIKE '%SOME CONDITION%'
```

Will restrict the included Artifacts to _only_ those that match the SQL condition.


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

