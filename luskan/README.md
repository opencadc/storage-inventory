# Storage Inventory TAP service

This service allows queries to the metadata of the Storage Inventory.

## Expected deployment
This service instance is expected to have:
- a proxy (HAproxy, apache, nginx) in front that performs SSL termination and forwards calls via HTTP on port 8080. Optional client certificates are passed through 
via the X-Client-Certificate HTTP header (http-request set-header X-Client-Certificate %[ssl_c_der,base64]
in haproxcy.cfg).
- a database backend to store the TAP metadata
- a reg service to resolve resource ids


The container expects that a directory is attached to /conf and containing the following:

## catalina.properties
System properties required by tomcat:

tomcat.connector.scheme=https

tomcat.connector.proxyName={SSL terminator host name}

tomcat.connector.proxyPort=443

### database connection pool configuration
luskan.tapadm.maxActive={}
luskan.tapadm.username={}
luskan.tapadm.password={}
luskan.tapadm.url=jdbc:postgresql://postgres/content
luskan.tapuser.maxActive={}
luskan.tapuser.username={}
luskan.tapuser.password={}
luskan.tapuser.url=jdbc:postgresql://postgres/content
luskan.uws.maxActive={}
luskan.uws.username={}
luskan.uws.password={}
luskan.uws.url=jdbc:postgresql://postgres/content

### optional intTest configuration
ca.nrc.cadc.reg.client.RegistryClient.host={SSL terminator host name}

### local authority map
###
### <base standardID> = <authority>
ivo://ivoa.net/std/GMS#groups-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/GMS#search-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#users-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#reqs-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#login-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#modpass-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#resetpass-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/UMS#whoami-0.1 = ivo://cadc.nrc.ca/gms           
ivo://ivoa.net/std/CDP#delegate-1.0 = ivo://cadc.nrc.ca/cred
ivo://ivoa.net/std/CDP#proxy-1.0 = ivo://cadc.nrc.ca/cred


## cadcproxy.pem 
This optional certificate is used to use to make some priviledged server-to-server calls (A&A support).

## cacerts
This optional directory includes CA certificates (pem format) are added to the system trust store.

## building it
docker build -t luskan -f Dockerfile .

## running it
docker run -d --volume=/path/to/external/conf:/conf:ro --volume=/path/to/external/logs:/logs:rw --link postgres --name luskan luskan

## checking it (while running)
docker exec -it luskan

## intTest
In a container environment, the intTest could run as follows:
1. Start postgres container (https://github.com/opencadc/docker-base/tree/master/cadc-postgresql-dev):
    docker run --rm -d --volume=/path/to/external/:/logs:rw --name postgres cadc-postgresql-dev:latest

2. Start haproxy container (https://github.com/opencadc/docker-base/tree/master/cadc-haproxy-dev)
    docker run --rm -d -v /path/to/external/:/logs:rw -v /path/to/external/:/conf:ro -p 443:443 --link luskan:cadc-service --link reg:reg-service --name haproxy cadc-haproxy-dev:latest

3. Start reg service using (https://github.com/opencadc/docker-base/tree/master/cadc-tomcat)
4. Start luskan as above
5. (CADC ONY) Set the $A environment variable to point to the directory where the CADC test certs are. 
6. Run command
gradle -Dca.nrc.cadc.reg.client.RegistryClient.host={SSL terminator host name}  -Dca.nrc.cadc.auth.BasicX509TrustManager.trust=True intTest