# Change Log

This is a cursory summary of changes to various storage-inventory components.
Check the README in specific modules for details.

## minoc:1.0.0
```
added optional `org.opencadc.minoc.trust.preauth` config
removed optional `org.opencadc.minoc.publicKeyFile` config
```
A `minoc` instance will download a public key from each trusted service and
use the public key(s) to validate URLs that include a _preauth_ token.

```
added optional `org.opencadc.minoc.readable` and `org.opencadc.minoc.writable` config
```
A `minoc` service will advertise (via inventory.StorageSite record in the database) the
_readable_ and _writable_ status; this information is synced to global inventory and
used by `raven` to determine if it should generate PUT or GET URLs that use the `minoc`
service(s) at that site. The configuration of _readGrantProvider_(s) and
_writeGrantProvider_(s) implicitly determines the status (_readable_ and _writable_
respectively); configuration of any _trust.preauth_ will also implicitly make make the 
status _readable_ and _writable_.

The explicit _readable_ and _writable_ configuration options will override the above 
implicit logic and set the status accordingly. This is currently optional but may be required
in a future version.

```
added optional config file: cadc-log.properties
added optional config file: cadc-vosi.properties
```

## raven:1.0.0
```
added org.opencadc.raven.inventory connection pool
```
A `raven` service uses this pool to perform database initialization. This pool is 
configured in the `catalina.properties` file.

```
added optional `org.opencadc.raven.keys.preauth` config
removed optional `org.opencadc.raven.publicKeyFile` and `org.opencadc.minoc.privateKeyFile` config
```
When configured to do so, a `raven` service will generate it's own public/private key pair
and use the private key to _sign_ `minoc` URLs. All the `minoc` services known to the global
`raven` service must also be configured to _trust_ `raven`.

```
added optional config file: cadc-log.properties
added optional config file: cadc-vosi.properties
```

## luskan:1.0.0
```
changed config file: cadc-tap-tmp.properties
```
A `luskan` service now uses the DelegatingStorageManager` so this config file must
specify which storage manager implementation to use along with existing 
implementation-specific configuration options.

```
added optional config file: cadc-log.properties
added optional config file: cadc-vosi.properties
```

## vault:0.5.0 (NEW)
This is a new service that implements the IVOA VOSpace 2.1 REST API to provide user-managed
hierarchical storage. `vault` is deployed with it's own database and an associated inventory
database, uses inventory services (`minoc`) for file storage and management.
