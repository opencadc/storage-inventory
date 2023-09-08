# Storage Inventory VOSpace quota support process (vault-quota)

Process to maintain container node sizes so that quota limits can be enforced by the 
main `vault` service. This process runs in incremental mode (single process running 
continuously) to update a local vospace database.

`vault-quota` is an optional process that is only needed if `vault` is configured to 
enforce quotas, although it could be used to maintain container node sizes without
quota enforcement.

## configuration
See the [cadc-java](https://github.com/opencadc/docker-base/tree/master/cadc-java) image 
docs for general config requirements.

Runtime configuration must be made available via the `/config` directory.

### vault-quota.properties
```
org.opencadc.vault.quota.logging = {info|debug}

# inventory database settings
org.opencadc.inventory.db.SQLGenerator=org.opencadc.inventory.db.SQLGenerator
org.opencadc.vault.quota.nodes.schema={schema for inventory database objects}
org.opencadc.vault.quota.nodes.username={username for inventory admin}
org.opencadc.vault.quota.nodes.password={password for inventory admin}
org.opencadc.vault.quota.nodes.url=jdbc:postgresql://{server}/{database}

org.opencadc.vault.quota.threads={number of threads to watch for artifact events}

# storage namespace
org.opencadc.vault.storage.namespace = {a storage inventory namespace to use}
```
The _nodes_ account owns and manages (create, alter, drop) vospace database objects and updates
content in the vospace schema. The database is specified in the JDBC URL. Failure to connect or 
initialize the database will show up in logs.

The _threads_ key configures the number of threads that watch for new Artifact events and initiate
the propagation of sizes to parent containers. These threads each monitor a subset of artifacts using
`Artifact.uriBucket` filtering; for simplicity, the following values are allowed: 1, 2, 4, 8, 16.

In addition to the above threads, there is one additional thread that propagates size changes up
the tree of container nodes to the container node(s) where quotas are specified.

## building it
```
gradle clean build
docker build -t vault-quota -f Dockerfile .
```

## checking it
```
docker run -it vault-quota:latest /bin/bash
```

## running it
```
docker run --user opencadc:opencadc -v /path/to/external/config:/config:ro --name vault-quota vault-quota:latest
```

