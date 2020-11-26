# Storage Inventory metadata-validate process (ringhold)

Process to validate metadata changes between storage sites and global inventory(ies). 

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

A file called `ringhold.properties` must be made available via the `/config` directory.

### fenwick.properties
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

TODO

### cadcproxy.pem -- UNUSED
In future, `ringhold` will use this certificate to authenticate to the remote (luskan) service.


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
