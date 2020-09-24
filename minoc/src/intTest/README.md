# minoc integration tests

The intTest suite is destructive!!

Runs tests against `ivo://cadc.nrc.ca/minoc` so that should resolve to a locally running minoc
instance using a test database. Direct database connection for setting up test content requires
an entry in $HOME/.dbrc with
```
MINOC_TEST cadctest {user} {pasword} {jdbc driver} {jdbc url}
```
which must connect to the same database as the minoc service.
