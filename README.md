# OpenCADC Storage Inventory System

The Storage Inventory system is designed to manage millions/billions of files for science
data archive.

What is it? [Concept Documentation](docs/)


# software components

## versions
For libraries (`cadc-{name}`) the version is in the `build.gradle` file. Libraries are published to
maven central under the `org.opencadc` groupId, for example `org.opencadc:cadc-inventory`. 

For services and agents, the version is in the `VERSION` file. Docker images are published to the
`images.opencadc.org` repository (currently a Harbor service).

## storage-inventory-dm
This is the storage inventory data model and architecture documentation. TODO: Add an FAQ.

## baldur
This is an implementation of the **permissions** service API using configurable rules to grant access 
based on resource identifiers (Artifact.uri values in the inventory data model).

Official docker image: `images.opencadc.org/storage-inventory/baldur:$VER`

## critwall
This is an implementation of the **file-sync** process that runs at a storage site and downloads files.

Official docker image: `images.opencadc.org/storage-inventory/critwall:$VER`

## fenwick
This is an implementation of the **metadata-sync** process that runs at both global inventory and at
storage sites.

Official docker image: `images.opencadc.org/storage-inventory/fenwick:$VER`

## luskan
This is an implementation of the **metadata** service that enables querying the storage inventory at
both global inventory and storage sites. It is an <a href="https://www.ivoa.net/documents/TAP/">IVOA TAP</a> 
service that supports ad-hoc querying of the inventory data model.

Official docker image: `images.opencadc.org/storage-inventory/luskan:$VER`

## minoc
This is an implementation of the **file** service that supports HEAD, GET, PUT, POST, DELETE operations
and <a href="https://www.ivoa.net/documents/SODA/">IVOA SODA</a> operations.

Official docker image: `images.opencadc.org/storage-inventory/minoc:$VER`

## raven
This is an implementation of the global **locator** service that supports transfer negotiation and direct
file GET requests.

Official docker image: `images.opencadc.org/storage-inventory/raven:$VER`

## ratik
This is an implementation of the **metadata-validate** process that runs at both global inventory and at
storage sites.

Official docker image: `images.opencadc.org/storage-inventory/ratik:$VER`

## ringhold
This is an implementation of a simplified part of the **metadata-validate** process that can be used 
to remove the local copy of artifacts from a site (file cleanup is done by `tantar`).

Official docker image: `images.opencadc.org/storage-inventory/ringhold:$VER`

## tantar
This is an implementation of the **file-validate** process that compares the inventory database against 
the back end storage at a storage site.

Official docker image: `images.opencadc.org/storage-inventory/tantar:$VER`

## vault
This is an implementation of an <a href="https://www.ivoa.net/documents/VOSpace/">IVOA VOSpace</a> 
service that uses storage-inventory as the back end storage mechanism.

Official docker image: `images.opencadc.org/storage-inventory/vault:$VER`

## cadc-*
These are libraries used in multiple services and applications.

- cadc-inventory: core data model implementation
- cadc-inventory-db: database library
- cadc-inventory-util: re-usable code
- cadc-inventory-server: re-usable service code
- cadc-storage-adapter: defines the interface between inventory and back end storage
- cadc-storage-adapter-fs: storage adapter implementation for a POSIX filesystem back end
- cadc-storage-adapter-ad: storage adapter for the legacy CADC Archive Directory storage system (temporary)
- cadc-storage-adapter-swift: storeage adapter implementation for the Swift Object Store API (e.g. CEPH Object Store)
- cadc-storage-adapter-test: re-usable test suite for storage adapter implementations
