# Storage Inventoty metadata-sync process integration tests (fenwick)

Integration tests can be run any time against a remote Luskan site with a local test inventory database.
## configuration
A file called `intTest.properties` can be in the classpath (in `src/intTest/resources`) to override properties.  See the
`cadc-inventory-db` project for more details.

### intTest.properties
```
server={server name in dbrc}
database={database name in dbrc}
schema={schema}
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
```

## running it
This expects a site with Luskan running to be available.  The Luskan URL can be obtained by having a
running registry service.  Set the Registry you would like to use with the 
`ca.nrc.cadc.reg.client.RegistryClient.host` System property.
```
gradle -i -Dca.nrc.cadc.reg.client.RegistryClient.host=testhost.cadc.dao.nrc.ca clean intTest
```
The registry at `testhost.cadc.dao.nrc.ca` will be used to look up the `luskanURI` property in the
`intTest.properties` file.  It will return a URL that can be used.  The tests will only pass if the
site has a single `StorageSite` set in the `storageSite` table.
