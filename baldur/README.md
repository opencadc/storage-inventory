# Rule-based permissions service (baldur)

This is an implementation of the <a href="https://github.com/opencadc/ac/tree/master/cadc-permissions-server">cadc-permissions-server</a> API that provides grants to read and write
artifacts on Storage Inventory. It was developed to be a `readGrantProvider` and
`writeGrantProvider` for `raven` and `minoc`, but could be used to grant access to other
resources.

## configuration

The following configuration files must be available in the /config directory.

### catalina.properties

This file contains java system properties to configure the tomcat server and some
of the java libraries used in the service.

See <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a>
for system properties related to the deployment environment.

See <a href="https://github.com/opencadc/core/tree/master/cadc-util">cadc-util</a>
for common system properties. 

`baldur` includes multiple IdentityManager implementations to support authenticated access:
  - See <a href="https://github.com/opencadc/ac/tree/master/cadc-access-control-identity">cadc-access-control-identity</a> for CADC access-control system support.
  - See <a href="https://github.com/opencadc/ac/tree/master/cadc-gms">cadc-gms</a> for OIDC token support.

### cadc-registry.properties

See <a href="https://github.com/opencadc/reg/tree/master/cadc-registry">cadc-registry</a>.

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
# - each entry has a name
# - an Artifact URI pattern (regex)
# - followed by entry-specific permission keys with a space separated list of group identifiers
org.opencadc.baldur.entry = {entry name}
{entry name}.pattern = {Artifact URI pattern}
{entry name}.anon = {true|false}
{entry name}.readOnlyGroup = {group URI}
{entry name}.readWriteGroup = {group URI}
```
`org.opencadc.baldur.grantExpiry` is used to calculate the expiry date of a grant. The value is an integer in seconds. The expiry date of a grant is: (the current time when a grant is issued + the number of seconds given by the expiryTime).

`org.opencadc.baldur.allowedUser` specifies the user(s) who are authorized to make calls to the service. The value is a list of user identities (e.g. X500 distingushed name), one line per user.

`org.opencadc.baldur.allowedGroup` specifies the group(s) whose members have authorization to make calls to the service. The value is a list of group identifiers (e.g. ivo://cadc.nrc.ca/gms?CADC), one line per group.

The `org.opencadc.baldur.entry` creates a new rule with the specified name. That name must be
unique and is used in the following four (4) dynamically defined keys.

The `{entry name}.pattern` entry (one per rule) specifies a regular expression to match against
resource URIs. For example, when used with storage-inventory services, these will match Artifact.uri values.

The `{entry name}.anon` flag specifies that all users (including anonymous) can read matching assets (default: false).

The `{entry name}.readOnlyGroup` entry specifies a group (one per line) that can read (get) matching assets (default: empty list).

The `{entry name}.readWriteGroup` entry specifies a group (one per line) that can read and write 
(get/put/update/delete) matching assets (default: empty list).

### example baldur.properties entry section:
```
org.opencadc.baldur.grantExpiry = 60

org.opencadc.baldur.allowedUser = cn=foo,ou=acme,o=example,c=com 
org.opencadc.baldur.allowedUser = cn=bar,ou=acme,o=example,c=com

org.opencadc.baldur.entry = TEST
TEST.pattern = ^cadc:TEST/.*
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

When more than one entry matches an artifact URI, the grants are combined as follows into one grant:
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

org.opencadc.baldur.entry = test
test.pattern = ^cadc:TEST/.*
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

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag baldur:latest baldur:$t
done
unset TAGS
docker image list baldur
```

