# minoc integration tests

The intTest suite is destructive!!

Runs tests against `ivo://opencadc.org/minoc` so that should resolve to a locally running minoc
instance using a test database. Direct database connection for setting up test content requires
an entry in $HOME/.dbrc with
```
MINOC_TEST {database} {user} {pasword} {jdbc driver} {jdbc url}
```
which must connect to the same database as the minoc service.

These tests also require the ability to resolve to the Vault VOSpace service to be able to download test data:

`ivo://cadc.nrc.ca/vault = https://ws-cadc.canfar.net/vault/capabilities`

