# Storage permissions service (baldur)

## building

```
gradle clean build
docker build -t baldur -f Dockerfile .
```

## checking it
```
docker run -it baldur:latest /bin/bash
```

## running it
```
docker run -d --volume=/path/to/external/config:/config:ro --volume=/path/to/external/logs:/logs:rw --name baldur baldur:latest
```

## configuration
See the <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> image docs 
for expected deployment and general config requirements.

The following configuration files must be available in the /config directory.

### baldur.properties

The configuration in baldur.properties serves two purposes:  to allow certain users access to the service, and to describe the permission rules for URIs that baldur will produce through its REST API.

The key `users` in baldor.properties is used to list the users who are authorized to make calls to the service.  The value is a space-separated list of distinguished names of the users allowed to make these calls.  The distinguished names come from the client certificates used to make the service call.  For example, this entry all allow client certificates with DNs CN=user1,OU=cadc,O=hia,C=ca and CN=user2,OU=cadc,O=hia,C=ca to call baldur.

```
users = CN=user1,OU=cadc,O=hia,C=ca CN=user2,OU=cadc,O=hia,C=ca
```

baldur.properties also describes the permissions rules this service reports as grants.  This consists of an unlimited number of entries (in the format `entry = <name> <regular expression>`) that are compared with the URI of the artifact in each permissions request.  When the artifact matches an entry, the grant information for that entry is applied.  

Each entry's grant information can be described with three other keys for that entry:
* `<entryname>.<anon> = <true|false>`
* `<entryname>.readOnlyGroups = <space separated list of groupURIs>`
* `<entryname>.readWriteGroups = <space separated list of groupURIs>`

An example permission entry in baldur.properites is:

```
entry = TEST ^cadc:TEST/.*
TEST.anon = true
TEST.readOnlyGroups = ivo://cadc.nrc.ca/gms?TestRead
TEST.readWriteGroups = ivo://cadc.nrc.ca/gms?TestWrite
```

In this example, for any artifact with a URI that matches `^cadc:TEST\\/.*`, the read grant will be:
* anonymous read not allowed
* readable by members of group TestRead and TestWrite

And the write grant will be:
* writable by members of group TestWrite

When more that one entry matches an artifact URI, the grants are combined as follows into one grant:
* if any of the matching entries allow anonymous read, anonymous read is allowed
* the members of any of the groups in all matching readOnlyGroup lists are allowed to read
* the members of any of the groups in all matching readWriteGroup lists are allowed to read and write

## Integration Testing

Client certificates named baldur-test-1.pem and baldur-test-2.pem must exist in directory $A/test-certificates 

In the running container, baldur-permissions-auth.properties must contain only the DN of the user identified by baldur-test-1.pem.  

In the running containter, baldur-permissions-config.properties should contain exactly the content shown in the example above. 
