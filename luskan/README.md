# Storage Inventory query service (luskan)

This service allows queries to the metadata of the Storage Inventory using
IVOA <a href="http://www.ivoa.net/documents/TAP/20190927/">TAP-1.1</a> web service API.

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
docker run -d --volume=/path/to/external/config:/config:ro --volume=/path/to/external/logs:/logs:rw --name luskan luskan:latest
```

## configuration
See the <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> image docs 
for expected deployment and general config requirements.

This service instance is expected to have a database backend to store the TAP metadata and which also includes the 
storage inventory tables. The container expects that a directory is attached to /conf and containing the following:

### catalina.properties
```
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
### LocalAuthority.properties
```
ivo://ivoa.net/std/GMS#search-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#users-0.1 = ivo://cadc.nrc.ca/gms                     
ivo://ivoa.net/std/CDP#delegate-1.0 = ivo://cadc.nrc.ca/cred
ivo://ivoa.net/std/CDP#proxy-1.0 = ivo://cadc.nrc.ca/cred
```
