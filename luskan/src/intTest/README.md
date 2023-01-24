# luskan integration tests

Runs tests against `ivo://opencadc.org/luskan` so that should resolve 
to a locally running luskan instance using a test database.

Client certificates named `luskan-test-auth.pem` and `luskan-test-noauth.pem` 
must exist in the directory $A/test-certificates.
The `luskan-test-auth.pem` user has authorization to call this service.
The `luskan-test-noauth.pem` user is not authorized to call this service.

The integration tests expect the following entry in `luskan.properties`.
`org.opencadc.luskan.allowedGroup={ group with member with identity in luskan-test-auth.pem }`
