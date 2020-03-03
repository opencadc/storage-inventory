# file-validate process (tantar)

Process to ensure validity of the information stored in the inventory database and back end storage at a storage site is
correct. This process is intended to be run periodically at a storage site to keep the site in a valid state.

## configuration

A file called `tantar.properties` must be made available via the `/config` directory.  All properties in the file are loaded and set as Java system properties.

### tantar.properties
```
org.opencadc.tantar.logging = {info|debug}

# set the bucket  or bucket prefix that tantar will validate
org.opencadc.tantar.bucket = {bucketname}

# set the policy to resolve conflicts of files
org.opencadc.tantar.resolutionPolicy = {STORAGE_IS_ALWAYS_RIGHT|INVENTORY_IS_ALWAYS_RIGHT}

# set whether to report all activity or to perform any actions required.
org.opencadc.tantar.reportOnly = {true|false}

## inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.tantar.schema={schema}
org.opencadc.tantar.username={dbuser}
org.opencadc.tantar.password={dbpassword}
org.opencadc.tantar.url=jdbc:postgresql://{server}/{database}

## storage adapter settings
org.opencadc.inventory.storage.StorageAdapter={fully-qualified-classname of implementation}
```
Additional java system properties and/or configuration files may be requires to configure the storage adapter.

## building it
This Docker image relies on the [Base Java Docker image](https://github.com/opencadc/docker-base/tree/master/cadc-java) built as an image called `cadc-java:latest`.

```
gradle -i clean build
docker build -t tantar -f Dockerfile .
```

## checking it
```
docker run -t tantar:latest /bin/bash
```

## running it
```
docker run -r --user nobody:nobody -v /path/to/external/config:/config:ro --name tantar tantar:latest
```

