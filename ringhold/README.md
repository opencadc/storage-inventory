# Storage Inventory local artifact removal process (ringhold)

Process to remove local artifacts that are no longer being synchronised by fenwick. This tool is used
to perform quick cleanup at a storage site after changing the fenwick artifact-filter policy.

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
```
The `inventory` account owns and manages all the content (insert, update, delete) in the inventory schema. Unlike
other components that modify inventory content, this component **does not initialise** the database objects because
it never makes sense to run this in a new/empty database. The database is specified in the JDBC URL. Failure to 
connect to a pre-initialised database will show up in logs.

### artifact-deselector.sql
Contains a SQL clause used as a WHERE constraint. The clause returns Artifact's that match the URI pattern.
```
WHERE uri LIKE 'cadc:CFHT/%'
```

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
