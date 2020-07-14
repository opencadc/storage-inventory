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

# list of users (one per line) who are allowed to call this service
org.opencadc.baldur.allowedUser = {user identity}

TODO: the allowedGroup property currently does not enable access to this service,
TODO: add support for allowedGroup to give service access.
# list of groups (one per line) whose members are allowed to call this service
org.opencadc.baldur.allowedGroup = {groupURI}

# one or more entry properties to grant permission to access artifacts
# - each entry has a name and an Artifact URI pattern (regex)
# - followed by entry-specific permission keys with a space separated list of group identifiers
org.opencadc.baldur.entry = {entry name} {regular expression}
{entry name}.anon = {true|false}
{entry name}.readOnlyGroup = {group URI} ...
{entry name}.readWriteGroup = {group URI} ...
```
`org.opencadc.baldur.grantExpiry` is used to calculate the expiry date of a grant. The value is an integer in seconds. The expiry date of a grant is: (the current time when a grant is issued + the number of seconds given by the expiryTime).

`org.opencadc.baldur.allowedUser` specifies the user(s) who are authorized to make calls to the service. The value is a list of user identities (e.g. X500 distingushed name), one line per user.

`org.opencadc.baldur.allowedGroup` specifies the group(s) whose members have authorization to make calls to the service. The value is a list of group identifiers (e.g. ivo://cadc.nrc.ca/gms?CADC), one line per group.

The `{entry name}.anon` flag specifies that all users (including anonymous) can get matching assets (default: false).

The `{entry name}.readOnlyGroup` entries specifies the group(s) that can get matching assets (default: empty list).

The `{entry name}.readWriteGroup` entries specifies the group(s) that can get/put/update/delete matching assets (default: empty list).

### example baldur.properites entry section:
```
org.opencadc.baldur.grantExpiry = 60

org.opencadc.baldur.allowedUser = cn=foo,ou=acme,o=example,c=com 
org.opencadc.baldur.allowedUser = bar,ou=acme,o=example,c=com

org.opencadc.baldur.entry = TEST ^cadc:TEST/.*
TEST.anon = false
TEST.readOnlyGroup = ivo://cadc.nrc.ca/gms?TestRead-1
TEST.readOnlyGroup = ivo://cadc.nrc.ca/gms?TestRead-2
TEST.readWriteGroup = ivo://cadc.nrc.ca/gms?TestWrite-1 
TEST.readWriteGroup = ivo://cadc.nrc.ca/gms?TestWrite-2
```

In this example the expiry time is 60 seconds. 

`foo` and `bar` are authorized users that can call this service.

Any artifact with a URI that matches `^cadc:TEST\\/.*`, the read grant will be:
* anonymous read not allowed
* readable by members of group TestRead-1, TestRead-2, TestWrite-1, and TestWrite-2

And the write grant will be:
* writable by members of group TestWrite-1 and TestWrite-2

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
org.opencadc.baldur.grantExpiry = 60

org.opencadc.baldur.allowedUser = { user identity for baldur-test-auth.pem }

org.opencadc.baldur.entry = test ^cadc:TEST/.*
test.anon = true
test.readOnlyGroups = ivo://cadc.nrc.ca/gms?Test-Read
test.readWriteGroups = ivo://cadc.nrc.ca/gms?Test-Write
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
