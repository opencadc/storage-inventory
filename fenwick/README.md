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
org.opencadc.fenwick.inventory.username={username for inventory admin}
org.opencadc.fenwick.inventory.password={password for inventory admin}
org.opencadc.fenwick.inventory.url=jdbc:postgresql://{server}/{database}

# Enable Storage Site lookup to synchronize site locations on Artifacts
# this should be true in global inventory and false in a storage site
org.opencadc.fenwick.trackSiteLocations = true | false

# remote inventory query service (luskan)
org.opencadc.fenwick.queryService={resourceID of remote TAP service with inventory data model}

# instance name
org.opencadc.fenwick.instanceName = {name}

# artifact selectivity
org.opencadc.fenwick.artifactSelector = all | filter

# event selectvitity
org.opencadc.fenwick.eventSelector = all | filter

# optional: threads (default: 1)
org.opencadc.fenwick.artifactThreads = 1 | 2 | 4 | 8

# time in seconds to retry processing after encountering an error.
org.opencadc.fenwick.maxRetryInterval={max sleep before retry}

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

If `artifactSelector` is `all` then fenwick harvests all artifacts from remote. If it is `filter` then fenwick harvests selected artifacts from remote as specified in `artifact-filter.sql` (see below). A global inventory and a
storage site that should get all Artifacts (files) would normally run with `all`. Specialised instances that want to
select a subset of all files would use the explicit filtering.

If `eventSelector` is `all` then all events (DeletedArtifactEvent, DeletedStorageLocationEvent, and
StorageLocationEvent) are synced. If the value is `filter` then fenwick harvests selected events from the remote
as specified in `event-filter.sql` (see below).
**NEW in 1.1**.

The `instanceName` is a simple name for this instance so it can track progress without interfering with other
instances. _Changing the name of an instance can discard progress tracking_ and cause the instance to start over (at
the beginning of time); it is possible (undocumented) to work around this, but there is currently no mechansim to do
this gracefully.
**NEW in 1.1**.

If `artifactThreads` is set, fenwick will run this number of threads when syncing artifacts. It will subdivide the
workload using the Artifact.uriBucket field so each thread has the same number of events to process. Setting this to
a value above 1 is not normally necessary, but can accelerate the building of a new storage site or global inventory 
in an existing system with many artifacts.
**NEW in 1.1**.

`maxRetryInterval` is the maximum number of seconds fenwick sleep between runs after encountering an error.
If fenwick encounters a non-fatal error, it sleeps for an initial timeout value, and runs again. 
If a subsequent run encounters an error, the previous timeout value is doubled, and fenwick sleeps before 
another run. This pattern repeats until `maxRetryInterval` is reached.

### cadcproxy.pem (optional)
Querying the remote query service (luskan) requires permission. `fenwick` uses this certificate file located
in /config to authenticate. If the file is not found, `fenwick` will make anonymous calls to the remote query
service.

### artifact-filter.sql (optional)
When `org.opencadc.fenwick.artifactSelector = filter` is specified, this config file
specifying the included Artifacts is required. The single clause in the SQL file *MUST* begin with the 
`WHERE` keyword, for example:

```sql
WHERE uri LIKE 'some:prefix/%'
```
can refer to any fields in the Artifact, and will restrict the included Artifacts to _only_ those that match the SQL
condition.

### event-filter.sql (optional)
When `org.opencadc.fenwick.eventSelector = filter` is specified, this config file
specifying the included events is required. The single clause in the SQL file *MUST* begin with the 
`WHERE` keyword, for example:

```sql
WHERE uri LIKE 'some:prefix/%'
```
can refer to any fields common to the various events (effectively, that means `uri` only), and will restrict the included events to _only_ those that match the SQL condition. If the event-filter.sql only restricts based on uri, then the same constraints can be applied in both files to implement namespace-based filtering.

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

