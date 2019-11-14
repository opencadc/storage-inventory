# storage-inventory
Storage Inventory Database Library

The intTest target runs tests against a backend RDBMS (code is portable, intTest classpath only
contains postgresql-jdbc for now).

The tests use $HOME/.dbrc to find connection info; by default they look for a server named "INVENTORY_TEST"
and database named "cadctest" and use teh default schema "inventory". Developers can create a file in this
directory named intTest.properties as follows to modify any/all of these values:

server = INVENTORY_TEST
database = cadctest
schema = inventory

Note that if you use schema = ${user} the Artifact table mayb collide with a caom2 table from other testing. Solution TBD.
