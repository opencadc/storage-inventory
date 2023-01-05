# Storage Inventory file-sync process (critwall)

Process to retrieve files from remote storage sites. This process is a hydrid multiprocess-multithread model: 
the operator runs multiple processes by  subdividing the Artifact.uriBucket space and multiple download threads 
within that process.

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.

### critwall.properties
```
org.opencadc.critwall.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.critwall.inventory.schema={schema for inventory database objects}
org.opencadc.critwall.inventory.username={username for inventory admin}
org.opencadc.critwall.inventory.password={password for inventory admin}
org.opencadc.critwall.inventory.url=jdbc:postgresql://{server}/{database}

# global transfer negotiation service (raven)
org.opencadc.critwall.locatorService={resourceID of global transfer negotiation service}

# storage back end
org.opencadc.inventory.storage.StorageAdapter={fully qualified class name for StorageAdapter implementation}

# file-sync
org.opencadc.critwall.buckets = {uriBucket prefix or range of prefixes}
org.opencadc.critwall.threads = {number of download threads}
```
The `inventory` account owns and manages (create, alter, drop) inventory database objects and manages
all the content (insert, update, delete) in the inventory schema. The database is specified in the JDBC URL. 
Failure to connect or initialize the database will show up in logs.

The range of uriBucket prefixes is specified with two values separated by a single - (dash) character; whitespace is ignored.

The number of download threads indirectly configures a database connection pool that is shared between file sync jobs
(approximately 3 threads per connection).

### cadcproxy.pem
Querying the global locator service (raven) and downloading files (minoc) may require permission.

### addtional configuration
Additional configuration file(s) may be needed for the StorageAdapter that is configured.

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
docker run --user opencadc:opencadc -v /path/to/external/config:/config:ro --name critwall critwall:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag critwall:latest critwall:$t
done
unset TAGS
docker image list critwall
```

