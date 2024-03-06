# Storage Inventory VOSpace-2.1 service (vault)

The simplest configuration that deploys `vault` with `minoc` with a single metadata database and single
back end storage system is sufficient to run the `vault` integration tests. The tests also relly on the
presence of the root owner X509 certificate in `build/classes/java/intTest/vault-test.pem`.
Some tests (primarily permission tests) will be skipped unless the certificate of a second user is present
in `build/classes/java/intTest/vault-auth-test.pem`. This user has to be member of the `ivo://cadc.nrc.ca/gms?opencadc-vospace-test`
group. The names of these certificates and groups are hardcoded in the `vault` int tests classes.

The int tests suite also relies on a specific configuration of the `vault` service:
### vault.properties
```
# service identity
org.opencadc.vault.resourceID = ivo://opencadc.org/vault

# (optional) identify which container nodes are allocations
org.opencadc.vault.allocationParent = /

# consistency settings
org.opencadc.vault.consistency.preventNotFound=true

# vault database settings
org.opencadc.vault.inventory.schema = inventory
org.opencadc.vault.vospace.schema = vault
org.opencadc.vault.singlePool = true

# root container nodes
org.opencadc.vault.root.owner = {owner of root node}

```