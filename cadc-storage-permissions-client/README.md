# storage-inventory
Storage Inventory Permissions Client

## integration testing

The PermissionsClient integration test uses certificates to authenticate baldur queries. 
The certificate X500 Distinguished Name (DN) must match the DN specified in the users properties in the baldur.properties files. 
A sample baldur.properties is in ..intTest/resources/testPermissionsClient directory.

The PermissionsClient integration test requires two certificates. 
The DN of one certificate is not in the baldur.properties file, and is used to test unauthorized access. 
The DN of the other certificate is in the baldur.properties file, and is used to test authorized access.
