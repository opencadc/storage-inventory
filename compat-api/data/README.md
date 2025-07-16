# long term backwards compat support of data-pub API 

This service is a backwards compatible data access API for the CADC. It provides
* mapping of simple `{archive}/{filename}` to storage inventory Artifact.uri values
used inn CADC archive data collections
* username/password authentication challenge (Basic auth) for non-public files

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image docs 
for expected deployment and general config requirements.

Runtime configuration must be made available via the `/config` directory.

### catalina.properties

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a>
for common system properties.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### cadc-log.properties (optional)
See <a href="https://github.com/opencadc/core/tree/master/cadc-log">cadc-log</a> for common 
dynamic logging control.

### cadc-vosi.properties (optional)
See <a href="https://github.com/opencadc/reg/tree/master/cadc-vosi">cadc-vosi</a> for common 
service state control.

### cadcproxy.pem
This client certificate is used to make server-to-server calls for system-level A&A purposes.

## building

```
gradle clean build
docker build -t data -f Dockerfile .
```

## checking it
```
docker run --rm -it data:latest /bin/bash
```

## running it
```
docker run --rm --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name data data:latest
```

