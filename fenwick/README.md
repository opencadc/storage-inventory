# Storage Inventory metadata-sync process (fenwick)

Process to sync metadata changes between storage sites and global inventory(ies). This process runs in
incremental mode (single process running continuously to update a local inventory database). 

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.

### fenwick.properties
```
org.opencadc.fenwick.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.fenwick.inventory.schema={schema for inventory database objects}
org.opencadc.fenwick.inventory.username={username for inventory admin}
org.opencadc.fenwick.inventory.password={password for inventory admin}
org.opencadc.fenwick.inventory.url=jdbc:postgresql://{server}/{database}

# Enable Storage Site lookup to synchronize site locations on Artifacts
org.opencadc.fenwick.trackSiteLocations={true|false}

# remote inventory query service (luskan)
org.opencadc.fenwick.queryService={resourceID of remote TAP service with inventory data model}

# selectivity
org.opencadc.fenwick.artifactSelector={all|filter}
```
The `inventory` account owns and manages (create, alter, drop) inventory database objects and manages
all the content (insert, update, delete) in the inventory schema. The database is specified in the JDBC URL. 
Failure to connect or initialize the database will show up in logs.

If the `trackSiteLocations` is `true`, fenwick will keep track of which remote site(s) have each Artifact; this
makes the destination database a "global" instance that knows where all the copies of an Artifact are located.
Set this to `false` when running in a storage site.

The `queryService` is the remote TAP service from which Artifacts are harvested. For a storage site, this is the
query service at the (a) global inventory. For a global inventory, this is the query service at a storage site; one
instance of fenwick is needed for each storage site.

Supported ArtifactSelector implementations: `org.opencadc.fenwick.AllArtifacts` (harvest all artifacts from
remote) and `org.opencadc.fenwick.IncludeArtifacts` (harvest seelcted artifacts from remote: see artifact-filter.sql 
below). A global inventory and a storage site that should get all Artitacts (files) would run with the AllArtifacts 
selector. Specialised instances that want to select a subset of all files would use the explicit filtering. 

### cadcproxy.pem
Querying the remote query service (luskan) requires permission. `fenwick` uses this certificate file located
in /config to authenticate.

### artifact-filter.sql (optional)
When `org.opencadc.fenwick.artifactSelector=filter` is specified, this config file
specifying the included Artifacts is required. The single clause in the SQL file *MUST* begin with the 
`WHERE` keyword.

> `artifact-filter.sql`
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
docker run --user opencadc:opencadc -v /path/to/external/config:/config:ro --name fenwick fenwick:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag fenwick:latest fenwick:$t
done
unset TAGS
docker image list fenwick
```
