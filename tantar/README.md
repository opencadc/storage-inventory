# Storage Inventory file-validate process (tantar)

Process to ensure validity of the information stored in the inventory database and back end storage at a storage site is
correct. This process is intended to be run periodically at a storage site to keep the site in a valid state.

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

A file called `tantar.properties` must be made available via the `/config` directory.  All properties in the file are loaded and set as Java system properties.
Additionally, a `cadcproxy.pem` file _may_ be required in the `/config` directory if iterations require authentication.

### tantar.properties
```
org.opencadc.tantar.logging = {info|debug}

# set the bucket prefix(es) that tantar will validate
org.opencadc.tantar.buckets = {bucket prefix or range of bucket prefixes}

# set the policy to resolve conflicts of files
org.opencadc.tantar.policy.ResolutionPolicy = {fully-qualified-classname of implementation}

# set whether to report all activity or to perform any actions required.
org.opencadc.tantar.reportOnly = {true|false}

## inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.tantar.db.schema={schema}
org.opencadc.tantar.db.username={dbuser}
org.opencadc.tantar.db.password={dbpassword}
org.opencadc.tantar.db.url=jdbc:postgresql://{server}/{database}

## storage adapter settings
org.opencadc.inventory.storage.StorageAdapter={fully-qualified-classname of implementation}
```
The database account owns and manages (create, alter, drop) inventory database objects and modifies the content. 
The database is specified in the JDBC URL. Failure to connect or initialize the database will show up in logs and cause
the application to exit.

Additional java system properties and/or configuration files may be requires to configure the storage adapter.

## building it
```
gradle clean build
docker build -t tantar -f Dockerfile .
```

## checking it
```
docker run -t tantar:latest /bin/bash
```

## running it
```
docker run -r --user opencadc:opencadc -v /path/to/external/config:/config:ro --name tantar tantar:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag tantar:latest tantar:$t
done
unset TAGS
docker image list tantar
```
