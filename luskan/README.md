# Storage Inventory query service (luskan)

This service allows queries to the metadata of the Storage Inventory using
IVOA <a href="http://www.ivoa.net/documents/TAP/20190927/">TAP-1.1</a> web service API.

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image docs 
for expected deployment and common config requirements.

This service instance is expected to have a database backend to store the TAP metadata and which also includes the 
storage inventory tables. The container expects that a directory is attached to /conf and containing the following:

### catalina.properties
```
# database connection pools
luskan.tapadm.maxActive={}
luskan.tapadm.username={}
luskan.tapadm.password={}
luskan.tapadm.url=jdbc:postgresql://{server}/{database}

luskan.tapuser.maxActive={}
luskan.tapuser.username={}
luskan.tapuser.password={}
luskan.tapuser.url=jdbc:postgresql://{server}/{database}

luskan.uws.maxActive={}
luskan.uws.username={}
luskan.uws.password={}
luskan.uws.url=jdbc:postgresql://{server}/{database}
```

### luskan.properties
```
# service identity
org.opencadc.luskan.resourceID=ivo://{authority}/{name}
org.opencadc.luskan.resultsDir={absolute path to directory for async results}

# true if luskan is running on a storage site, false or not set if
# running on a global site
org.opencadc.luskan.isStorageSite={true|false}
```

### LocalAuthority.properties
The LocalAuthority.properties file specifies which local service is authoritative for various site-wide functions. The keys
are standardID values for the functions and the values are resouceID values for the service that implements that standard 
feature.

Example:
```
ivo://ivoa.net/std/GMS#search-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#users-0.1 = ivo://cadc.nrc.ca/gms    
ivo://ivoa.net/std/UMS#login-0.1 = ivo://cadc.nrc.ca/gms           

ivo://ivoa.net/std/CDP#delegate-1.0 = ivo://cadc.nrc.ca/cred
ivo://ivoa.net/std/CDP#proxy-1.0 = ivo://cadc.nrc.ca/cred
```

## building it
```
gradle clean build
docker build -t luskan -f Dockerfile .
```

## checking it
```
docker run -it luskan:latest /bin/bash
```

## running it
```
docker run --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name luskan luskan:latest
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
