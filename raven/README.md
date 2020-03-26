# Storage Inventory locate service (raven)
This service implements transfer negotiation in a global inventory deployment.

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image docs 
for expected deployment and general config requirements.

### catalina.properties
When running raven.war in tomcat, parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:
```
# database connection pools
raven.invadm.maxActive={}
raven.invadm.username={}
raven.invadm.password={}
raven.invadm.url=jdbc:postgresql://{server}/{database}

```

### raven.properties
The following keys (with example values) are needed:

```
# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.inventory.db.schema=inventory

# permission granting service settings
org.opencadc.inventory.permissions.ReadGrant.serviceID=ivo://{authority}/{name}
```

## building

```
gradle clean build
docker build -t raven -f Dockerfile .
```

## checking it
```
docker run -it raven:latest /bin/bash
```

## running it
```
docker run --user tomcat:tomcat --volume=/path/to/external/conf:/conf:ro --name raven raven:latest
```
