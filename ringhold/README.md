# Storage Inventory local artifact deletion process (ringhold)

Process to remove the local copy of artifacts from a storage site inventory database and 
generate DeletedStorageLocationEvent(s) so the removal will propagate correctly to a global inventory. 
This does not remove the files from storage (see `tantar`).

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.

### ringhold.properties
```
org.opencadc.ringhold.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.ringhold.inventory.schema={schema for inventory database objects}
org.opencadc.ringhold.inventory.username={username for inventory admin}
org.opencadc.ringhold.inventory.password={password for inventory admin}
org.opencadc.ringhold.inventory.url=jdbc:postgresql://{server}/{database}

# artifact namespace(s) to remove
org.opencadc.ringhold.namespace={storage site namespace}

# artifact uri bucket filter (optional)
org.opencadc.ringhold.buckets={uriBucket prefix or range of prefixes}
```
The `inventory` account owns and manages all the content (insert, update, delete) in the inventory schema. Unlike
other components that modify inventory content, this component **does not initialise** the database objects because
it never makes sense to run this in a new/empty database. The database is specified in the JDBC URL. Failure to 
connect to a pre-initialised database will show up in logs.

The `namespace` is the prefix of the Artifact URI's to be deleted. The `namespace` must end with a colon (:) 
or slash (/) so one namespace cannot accidentally match (be a prefix of) another namespace. Multiple values 
of `namespace` may be specified, one per line.

The `buckets` value indicates a subset of artifacts to delete. The range of uri bucket prefixes is specified 
with two values separated by a single - (dash) character; whitespace is ignored. Multiple instances of `ringhold` 
can be run (in parallel) to subdivide the work as long as the range of buckets do not overlap.

## building it
```
gradle clean build
docker build -t ringhold -f Dockerfile .
```

## checking it
```
docker run -it ringhold:latest /bin/bash
```

## running it
```
docker run --rm --user opencadc:opencadc -v /path/to/external/config:/config:ro --name ringhold ringhold:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag ringhold:latest ringhold:$t
done
unset TAGS
docker image list ringhold
```
