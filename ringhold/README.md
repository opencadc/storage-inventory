# Storage Inventory local artifact removal process (ringhold)

Process to remove local artifacts that are no longer being synchronised by fenwick. This tool is used
to perform quick cleanup at a storage site after changing the fenwick artifact-filter policy.

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

A file called `ringhold.properties` must be made available via the `/config` directory.

### ringhold.properties
```
org.opencadc.ringhold.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.ringhold.db.schema={schema}
org.opencadc.ringhold.db.username={dbuser}
org.opencadc.ringhold.db.password={dbpassword}
org.opencadc.ringhold.db.url=jdbc:postgresql://{server}/{database}
```

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
