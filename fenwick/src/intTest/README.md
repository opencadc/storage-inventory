# Storage Inventoty metadata-sync process integration tests (fenwick)

Integration tests can be run any time against a remote Luskan site with a local test inventory database.
## configuration
A file called `intTest.properties` can be in the classpath (in `src/intTest/resources`) to override properties.  See the
`cadc-inventory-db` project or the [org.opencadc.fenwick.TestUtil](src/main/intTest/org/opencadc/fenwick/TestUtil) class for more details.

### intTest.properties
```
inventoryServer={inventory database server name in dbrc}
inventoryDatabase={inventory database name in dbrc}
inventorySchema={inventory database schema}
luskanServer={server of luskan site}
luskanSchema={luskan database schema}
luskanDatabase={name of luskan database}
luskanURI={uri of luskan site to pull metadata from}
```

### cadcproxy.pem
Querying the remote query service (luskan) requires permission. `fenwick` uses the `~/.ssl/cadcproxy.pem`
file to authenticate.

## building it
```
gradle clean build
docker build -t fenwick -f Dockerfile .
```

## checking it
```
docker run -it fenwick:latest /bin/bash
docker run -it luskan:latest /bin/bash
```

## running it
> :warning: **These tests are destructive and will destroy everything in your databases!**:

A `fenwick.properties` file is not required as the tests bypass all the configuration.

The integration tests require the following services to be running.  Bundling this with a `docker-compose.yml` file will ease reproducing it:
  - A PostgreSQL instance to represent the local inventory database.  Needs to be network accessible by the test.
  - A PostgreSQL instance to represent the Luskan (remote) inventory database.  Needs to be network accessible to the test.
  - A TAP (Luskan) service that uses the Luskan PostgreSQL above as its backend.  Needs to be network accessible to the test's TAP Client.
  - A Registry service for the test to find the TAP (Luskan) service.  It should be configured to have the value of `org.opencadc.fenwick.TestUtil.LUSKAN_URI` point to the Luskan service above.  
    The Registry client defaults to use the main CADC service, but can be overridden using the `-Dca.nrc.cadc.reg.client.RegistryClient.host' System property.
  - A proxy service to route traffic on the default ports.  This is mostly convenience but highly recommended as it will simplify configuring the Registry service above.
  - A `~/.dbrc` file containing entries that hold secrets on connecting to your databases above.  
    - In this example, there are two databases on the same host for simplicity on different ports.  The default database name that is looked up in 
    the `org.opencadc.fenwick.TestUtil` class is `cadctest`.  Replace the `USERNAME` and `PASSWORD` with your own:
```
INVENTORY_TEST	cadctest	USERNAME	PASSWORD	org.postgresql.Driver	jdbc:postgresql://testdb.cadc.dao.nrc.ca:15432/cadctest
LUSKAN_TEST	cadctest	USERNAME	PASSWORD	org.postgresql.Driver	jdbc:postgresql://testdb.cadc.dao.nrc.ca:25432/cadctest
```

For example:
```
gradle -i -Dca.nrc.cadc.reg.client.RegistryClient.host=testhost.cadc.dao.nrc.ca clean intTest
```
The registry at `testhost.cadc.dao.nrc.ca` will be used to look up the Luskan service.
