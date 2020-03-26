# Storage Inventory permissions service (baldur)

## configuration
See the <a href="https://github.com/opencadc/docker-base/tree/master/cadc-tomcat">cadc-tomcat</a> image docs 
for expected deployment and general config requirements.

The following configuration files must be available in the /config directory.

### baldur.properties

The configuration in baldur.properties serves two purposes:  to allow certain users access to the service and to describe the permission rules for URIs that baldur will produce through its REST API.
```
# space separated list of users who are allowed to call this service
users = {user identity} ...

# space separated list of groups who are allowed to call this service
groups = {group URI} ...

# one or more entry properties to grant permission to access artifacts
# - each entry has a name and an Artifact URI pattern (regex)
# - followed by entry-specific permission keys with a space separated list of group identifiers
entry = {entry name} {regular expression}`
{entry name}.anon = {true|false}
{entry name}.readOnlyGroups = {group URI} ...
{entry name}.readWriteGroups = {group URI} ...
```

The `users` value is used to specify the user(s) who are authorized to make calls to the service. The value is a space-separated list of user identities (e.g. X500 distingushed name).

The `groups` value is used to specify the group(s) who are authorized to make calls to the service. The value is a space-separated list of group identifiers (e.g. ivo://cadc.nrc.ca/gms?CADC).

The `{entry name}.anon` flag specifies that all users (including anonymous) can get matching artifacts (default: false).

The `{entry name}.readOnlyGroups` list specifies the group(s) that can get matching artifacts (default: empty list).

The `{entry name}.readWriteGroups` list specifies the group(s) that can get/put/update/delete matching artifacts (default:
empty list).

### example baldur.properites entry section:
```
entry = TEST ^cadc:TEST/.*
TEST.anon = false
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


## integration testing

Client certificates specified in the users property in the baldur.properties must exist in the directory $A/test-certificates. 


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
