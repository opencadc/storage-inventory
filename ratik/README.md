# Storage Inventory metadata-validate process (ratik)

Process to validate metadata (sets of artifacts) between storage sites and global inventory(ies).

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image docs for general config requirements.

A file called `ratik.properties` must be made available via the `/config` directory.

### ratik.properties
```
org.opencadc.ratik.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.ratik.inventory.schema={schema}
org.opencadc.ratik.inventory.username={dbuser}
org.opencadc.ratik.inventory.password={dbpassword}
org.opencadc.ratik.inventory.url=jdbc:postgresql://{server}/{database}

# artifact uri bucket filter
org.opencadc.ratik.buckets={uriBucket prefix or range of prefixes}

# selectivity
org.opencadc.ratik.artifactSelector={all|filter}

# remote inventory query service (luskan)
org.opencadc.ratik.queryService={resourceID of remote TAP service with inventory data model}

# local site type, true = global site, false = storage site
org.opencadc.ratik.trackSiteLocations={true|false}
```

The range of uri bucket prefixes is specified with two values separated by a 
single - (dash) character; whitespace is ignored. Multiple instances of `ratik` 
can be run (in parallel) to subdivide the work as long as the range of buckets 
do not overlap.

The `artifactSelector`, `queryService`, and `trackSiteLocations` settings have the 
same meaning and effect as in `fenwick` and must be configured the same when `ratik` 
is used to validate an instance maintained by `fenwick`.

### cadcproxy.pem
Remote calls will use this certificate to authenticate.

### artifact-filter.sql (optional)
When the `org.opencadc.ratik.artifactSelector = filter` is specified, this config 
file specifying the included Artifacts is required. The single clause in the SQL 
file *MUST* begin with the `WHERE` keyword. This should be the same artifact filter
as used in fenwick.

> `artifact-filter.sql`
```sql
WHERE uri LIKE '%SOME CONDITION%'
```

Will restrict the included Artifacts to _only_ those that match the SQL condition.


## building it
```
gradle clean build
docker build -t ratik -f Dockerfile .
```

## checking it
```
docker run -it ratik:latest /bin/bash
```

## running it
```
docker run --rm --user opencadc:opencadc -v /path/to/external/config:/config:ro --name ratik ratik:latest
```

## apply version tags
```bash
. VERSION && echo "tags: $TAGS" 
for t in $TAGS; do
   docker image tag ratik:latest ratik:$t
done
unset TAGS
docker image list ratik
```
