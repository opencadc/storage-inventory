# Storage Inventory locate service (raven)
This service implements transfer negotiation in a global inventory deployment.

## configuration
See the [cadc-tomcat](https://github.com/opencadc/docker-base/tree/master/cadc-tomcat) image docs 
for expected deployment and general config requirements.

Runtime configuration must be made available via the `/config` directory.

### catalina.properties
When running raven.war in tomcat, parameters of the connection pool in META-INF/context.xml need
to be configured in catalina.properties:
```
# query pool for user requests
org.opencadc.raven.query.maxActive={max connections for query pool}
org.opencadc.raven.query.username={database username for query pool}
org.opencadc.raven.query.password={database password for query pool}
org.opencadc.raven.query.url=jdbc:postgresql://{server}/{database}

# admin pool for setup
org.opencadc.raven.inventory.maxActive={max connections for query pool}
org.opencadc.raven.inventory.username={database username for query pool}
org.opencadc.raven.inventory.password={database password for query pool}
org.opencadc.raven.inventory.url=jdbc:postgresql://{server}/{database}
```
The _query_ account is used to query inventory for the requested Artifact; this pool can be
configured with a read-only database account.

The _inventory_ account owns and manages (create, alter, drop) inventory database objects and 
(optional) URL signing keys (see _keys.preauth_ below).

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

### raven.properties
The following keys are required:
```
# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.raven.inventory.schema={schema}

# consistency settings
org.opencadc.raven.consistency.preventNotFound=true|false
```
The _preventNotFound_ key can be used to configure `raven` to prevent artifact-not-found errors that might 
result due to the eventual consistency nature of the system by directly checking for the artifact at 
_all known_ sites. This feature introduces an overhead for the genuine not-found cases.



The following optional keys configure raven to use external service(s) to obtain grant information in order
to perform authorization checks and generate signed URLs:
```
org.opencadc.raven.readGrantProvider={resourceID of a permission granting service}
org.opencadc.raven.writeGrantProvider={resourceID of a permission granting service}

# url signing key usage
org.opencadc.raven.keys.preauth={true|false}
```
The optional _readGrantProvider_ and _writeGrantProvider_ keys configure minoc to call other services to get grants (permissions) for 
operations. Multiple values of the permission granting service resourceID(s) may be provided by including multiple property 
settings. All services will be consulted but a single positive result is sufficient to grant permission for an 
action.

The _keys.preauth_ key (default: false) configures `raven` to use URL-signing. When enabled, `raven` can generate a signed token
and embed it into the URL; `minoc` services that are configured to trust a `raven` service will download the public key and can 
validate the token and grant access without further permission checks. With transfer negotiation, the signed URL gets added as 
an additional "anonymous" URL.

The following optional keys configure raven to prioritize sites returned in transfer negotiation, with higher priority
sites first in the list of transfer URL's. Multiple values of _namespace_ may be specified for a single _resourceID_. 
The _namespace_ value(s) must end with a colon (:) or slash (/) so one namespace cannot accidentally match (be a 
prefix of) another namepsace.

```
org.opencadc.raven.putPreference={entry name}
{entry name}.resourceID={storage site resourceID}
{entry name}.namespace={storage site namespace}
```

Example `putPreference` config:
```
org.opencadc.raven.putPreference=CADC
CADC.resourceID=ivo://cadc.nrc.ca/cadc/minoc
CADC.namespace=cadc:IRIS/
CADC.namespace=cadc:CGPS/
```

The _putPreference_ rules are optimizations; they do not restrict the destination of a PUT. They are useful in cases 
where a namespace is intended to be stored only in some site(s) or where most or all PUTs come from systems that are near one 
storage site. TODO: support for `GET` preferences? also use client IP address to prioritize?

`raven` can be configured to return URLs to external sites (in addition to the `minoc` ones) for the artifacts that are
mirrored at other data providers. To do that, the configuration needs to include the name of the storage resolver class
that implements the `ca.nrc.cadc.net.StorageResolver` interface and turns an artifact URI into a corresponding
external URL. For example: 

```
ca.nrc.cadc.net.StorageResolver=ca.nrc.cadc.caom2.artifact.resolvers.MastResolver
```

**For developer testing only:** To disable authorization checking (via `readGrantProvider` or `writeGrantProvider`
services), add the following configuration entry to raven.properties:
```
org.opencadc.raven.authenticateOnly=true
```
When _authenticateOnly_ is `true`, any authenticated user will be able to read/write/delete files and anonymous users
will be able to read files.

### cadc-log.properties (optional)
See <a href="https://github.com/opencadc/core/tree/master/cadc-log">cadc-log</a> for common 
dynamic logging control.

### cadcproxy.pem (optional)
This client certificate is used to make authenticated server-to-server calls for system-level A&A purposes.

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
docker run --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name raven raven:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag raven:latest raven:$t
done
unset TAGS
docker image list raven
```
