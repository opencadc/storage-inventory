# Storage Inventory file-sync process (critwall)

Process to retrieve files from remote storage sites. This process is a hydrid multiprocess-multithread model: 
the operator runs multiple processes by  subdividing the Artifact.uriBucket space and multiple download threads 
within that process.

## configuration
The following configuration files must be available in the `/config` directory.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### critwall.properties
```
org.opencadc.critwall.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.critwall.inventory.schema={schema for inventory database objects}
org.opencadc.critwall.inventory.username={username for inventory admin}
org.opencadc.critwall.inventory.password={password for inventory admin}
org.opencadc.critwall.inventory.url=jdbc:postgresql://{server}/{database}

# global transfer negotiation service (raven) or a peer files service (minoc)
org.opencadc.critwall.locatorService={resourceID of service to find source files}

# storage back end
org.opencadc.inventory.storage.StorageAdapter={fully qualified class name for StorageAdapter implementation}

# file sync
org.opencadc.critwall.buckets = {Artifact.uriBucket prefix or range of prefixes}
org.opencadc.critwall.threads = {number of download threads}
```
The `inventory` account owns and manages (create, alter, drop) inventory database objects and manages
all the content (insert, update, delete) in the inventory schema. The database is specified in the 
JDBC URL. Failure to connect or initialize the database will show up in logs.

The _locatorService_ is normally the global locator (raven) service that supports transfer negotiation.
Despite the config name, this can also be a specific files (minoc) service to retrieve from, although
this configuration is likely to result in some failed transfers (not found) if there are more than two
sites in the storage system and artifacts+files have not been synced to the target service.

The range of _uriBucket_ prefixes is specified with two values separated by a single - (dash) 
character; whitespace is ignored. A value `0-f` is equivalent to _all_ artifacts. Subsets of this
range are used to subdivide the workload between multiple instances of `critwall` so they work on
different subsets of artifacts.

The number of download threads indirectly configures a database connection pool that is shared 
between file sync jobs (approximately 3 threads per connection). The useful maximum depends on the
network connectivity between sites: higher latency may require more threads to make full use of the
available bandwidth.

`critwall` includes the following StorageAdapter implementations:
- see https://github.com/opencadc/storage-adapter/tree/master/cadc-storage-adapter-fs">cadc-storage-adapter-fs</a> to store files in a local file system
- see https://github.com/opencadc/storage-adapter/tree/master/cadc-storage-adapter-swift">cadc-storage-adapter-swift</a> to store files in an object store using the Swift API

### cadcproxy.pem (optional)

Querying the global locator service (raven) and downloading files (minoc) may require permission.
If the file is not found, `critwall` will make anonymous calls to the remote locator service and
attempt anonymous downloads.

See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs 
for general config requirements.


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

