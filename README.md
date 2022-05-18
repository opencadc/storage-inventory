# storage-inventory
Storage Inventory components

## storage-inventory-dm
This is the storage inventory data model and architecture documentation.

## cadc-*
These are libraries used in multiple services and applications.

## baldur
This is an implementation of the **cadc-permissions** service API.

## critwall
This is an implementation of the **file-sync** process that runs at a storage site and downloads files.

## fenwick
This is an implementation of the **metadata-sync** process that runs at both global inventory and at
storage sites.

## luskan
This is an implementation of the IVOA **TAP** service that enables querying the storage inventory at
both global inventory and storage sites.

## minoc
This is an implementation of the **file** service that supports HEAD, GET, PUT, POST, DELETE operations
and IVOA SODA operations.

## raven
This is an implementation of the global **locator** service that supports transfer negotiation.

## ratik
This is an implementation of the **metadata-validate** process that runs at both global inventory and at
storage sites.

## ringhold
This is an implementation of part of the **metadata-validate** process that cleans up after a change in 
local (fenwick) filter policy. This is used if the new fenwick policy excludes artifacts previously
synced.

## tantar
This is an implementation of the **file-validate** process that compares the inventory database against 
the back end storage at a storage site.
