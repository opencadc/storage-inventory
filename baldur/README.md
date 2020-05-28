# Storage Inventory permissions service (baldur)

## configuration
See the <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> image docs 
for expected deployment and general config requirements.

The following configuration files must be available in the /config directory.

### baldur.properties

The configuration in baldur.properties serves two purposes:  to allow certain users access to the service and to describe the permission rules for URIs that baldur will produce through its REST API.
```
# time (in seconds) the grant is considered valid 
org.opencadc.baldur.grantExpiry = {time in seconds}

# space separated list of users who are allowed to call this service
org.opencadc.baldur.allowedUser = {user identity}

TODO: add support for allowedGroup
# space separated list of groups who are allowed to call this service
org.opencadc.baldur.allowedGroup = {groupURI}

# one or more entry properties to grant permission to access artifacts
# - each entry has a name and an Artifact URI pattern (regex)
# - followed by entry-specific permission keys with a space separated list of group identifiers
entry = {entry name} {regular expression}`
{entry name}.anon = {true|false}
{entry name}.readOnlyGroups = {group URI} ...
{entry name}.readWriteGroups = {group URI} ...
```
The `expiryTime` value is used to calculate the expiry date of a grant. The value is an integer in seconds. The expiry date of a grant is: (the current time when a grant is issued + the number of seconds given by the expiryTime).

The `users` value is used to specify the user(s) who are authorized to make calls to the service. The value is a space-separated list of user identities (e.g. X500 distingushed name).

The `groups` value is used to specify the group(s) who are authorized to make calls to the service. The value is a space-separated list of group identifiers (e.g. ivo://cadc.nrc.ca/gms?CADC).

The `{entry name}.anon` flag specifies that all users (including anonymous) can get matching artifacts (default: false).

The `{entry name}.readOnlyGroups` list specifies the group(s) that can get matching artifacts (default: empty list).

The `{entry name}.readWriteGroups` list specifies the group(s) that can get/put/update/delete matching artifacts (default:
empty list).

### example baldur.properites entry section:
```
expiryTime = 60

users = cn=foo,ou=acme,o=example,c=com cn=bar,ou=acme,o=example,c=com

entry = TEST ^cadc:TEST/.*
TEST.anon = false
TEST.readOnlyGroups = ivo://cadc.nrc.ca/gms?TestRead-1 ivo://cadc.nrc.ca/gms?TestRead-2
TEST.readWriteGroups = ivo://cadc.nrc.ca/gms?TestWrite-1 ivo://cadc.nrc.ca/gms?TestWrite-2
```

In this example the expiry time is 60 seconds. 

The users `foo` and `bar` are authorized to call this service.

Any artifact with a URI that matches `^cadc:TEST\\/.*`, the read grant will be:
* anonymous read not allowed
* readable by members of group TestRead and TestWrite

And the write grant will be:
* writable by members of group TestWrite

When more that one entry matches an artifact URI, the grants are combined as follows into one grant:
* if any of the matching entries allow anonymous read, anonymous read is allowed
* the members of any of the groups in all matching readOnlyGroup lists are allowed to read
* the members of any of the groups in all matching readWriteGroup lists are allowed to read and write


## integration testing

Client certificates named `baldur-test-auth.pem` and `baldur-test-noauth.pem` must exist in the directory $A/test-certificates.
The `baldur-test-auth.pem` must belong to a `users` identity given in `baldur.properties`. This user has authorization to call this service to retrieve permissions.
The `baldur-test-noauth.pem` is for a user not listed in `users` and therefore is not authorized to call this service.

The integration tests expect the following entry in `baldur.properties`. 
```
expiryTime = 60

users = { user identity for baldur-test-auth.pem }

entry = test ^cadc:TEST/.*
test.anon = true
test.readOnlyGroups = ivo://cadc.nrc.ca/gms?TestRead
test.readWriteGroups = ivo://cadc.nrc.ca/gms?TestWrite
```

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
docker run --user tomcat:tomcat --volume=/path/to/external/config:/config:ro --name baldur baldur:latest
```
