# Storage Inventory query service (luskan)

This service allows queries to the metadata of the Storage Inventory using
IVOA <a href="http://www.ivoa.net/documents/TAP/20190927/">TAP-1.1</a> web service API.

### deployment
The `luskan` war file can be renamed at deployment time in order to support an alternate
service name, including introducing additional path elements (see war-rename.conf).

This service instance is expected to have a database backend to store the TAP metadata and which
also includes the storage inventory tables.

### configuration
The following configuration files must be available in the /config directory.

### catalina.properties
This file contains java system properties to configure the tomcat server and some of the java libraries used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a>
for common system properties.

`luskan` includes multiple IdentityManager implementations to support authenticated access:
 - See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.

 - See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.


`luskan` requires 3 connections pools:

```
# database connection pools
org.opencadc.luskan.uws.maxActive={max connections for jobs pool}
org.opencadc.luskan.uws.username={database username for jobs pool}
org.opencadc.luskan.uws.password={database password for jobs pool}
org.opencadc.luskan.uws.url=jdbc:postgresql://{server}/{database}

org.opencadc.luskan.tapadm.maxActive={max connections for TAP admin pool}
org.opencadc.luskan.tapadm.username={username for TAP admin pool}
org.opencadc.luskan.tapadm.password={password for TAP admin pool }
org.opencadc.luskan.tapadm.url=jdbc:postgresql://{server}/{database}

org.opencadc.luskan.query.maxActive={max connections for user query pool}
org.opencadc.luskan.query.username={username for user query pool}
org.opencadc.luskan.query.password={password for user query pool}
org.opencadc.luskan.query.url=jdbc:postgresql://{server}/{database}
```
The `tapadm` pool manages (create, alter, drop) tap_schema tables and manages the tap_schema content. The `uws` 
pool manages (create, alter, drop) uws tables and manages the uws content (creates and modifies jobs in the uws
schema when jobs are created and executed by users.

The `query` pool is used to run TAP queries, including creating tables in the tap_upload schema. 

All three pools must have the same JDBC URL (e.g. use the same database) with PostgreSQL. This may be relaxed in future.
In addition, the TAP service does not currently support a configurable schema name: it assumes a schema named `inventory`
holds the content.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.


### cadc-tap-tmp.properties
Temporary storage of async results is now handled by the 
[cadc-tap-tmp](https://github.com/opencadc/tap/tree/master/cadc-tap-tmp) library. This
library should be configured as follows:
```
org.opencadc.tap.tmp.TempStorageManager.baseURL = https://{server name}/{luskan path}/results
org.opencadc.tap.tmp.TempStorageManager.baseStorageDir = {local directory}
```

### luskan.properties
```
# true if luskan is running on a storage site, false or not set if
# running on a global site
org.opencadc.luskan.isStorageSite={true|false}

# optional authorization: allow anonymous queries (default: false for backwards compatibility)
org.opencadc.luskan.allowAnon={true|false}

# optional: group(s) whose members have authorization to query luskan 
org.opencadc.luskan.allowedGroup={authorized group}
```

`org.opencadc.luskan.allowedGroup` specifies the group(s) whose members have authorization to make calls 
to the service. The value is a list of group identifiers (e.g. ivo://cadc.nrc.ca/gms?CADC), one line per
group.


## building it
```
gradle clean build
docker build -t luskan -f Dockerfile .
```

## checking it
```
docker run --rm -it luskan:latest /bin/bash
```

## running it
```
docker run --rm --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name luskan luskan:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag luskan:latest luskan:$t
done
unset TAGS
docker image list luskan
```
