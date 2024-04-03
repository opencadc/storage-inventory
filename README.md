# OpenCADC Storage Inventory System

The Storage Inventory system is designed to manage millions/billions of files for science
data archive.

What is it? [Concept Documentation](docs/)

# software components

## storage-inventory-dm
This is the storage inventory data model and architecture documentation. TODO: Add an FAQ.

## baldur
This is an implementation of the **permissions** service API using configurable rules to grant access 
based on resource identifiers (Artifact.uri values in the inventory data model).

## critwall
This is an implementation of the **file-sync** process that runs at a storage site and downloads files.

## fenwick
This is an implementation of the **metadata-sync** process that runs at both global inventory and at
storage sites.

## luskan
This is an implementation of the **metadata** service that enables querying the storage inventory at
both global inventory and storage sites. It is an <a href="https://www.ivoa.net/documents/TAP/">IVOA TAP</a> 
service that supports ad-hoc querying of the inventory data model.

## minoc
This is an implementation of the **file** service that supports HEAD, GET, PUT, POST, DELETE operations
and <a href="https://www.ivoa.net/documents/SODA/">IVOA SODA</a> operations.

## raven
This is an implementation of the global **locator** service that supports transfer negotiation and direct
file GET requests.

## ratik
This is an implementation of the **metadata-validate** process that runs at both global inventory and at
storage sites.

## ringhold
This is an implementation of part of the **metadata-validate** process that cleans up after a change in 
local artifact selection policy. This is used if the new fenwick policy excludes artifacts previously synced.

## tantar
This is an implementation of the **file-validate** process that compares the inventory database against 
the back end storage at a storage site.

## vault
UNDER DEVELOPMENT: This is an implementation of an <a href="https://www.ivoa.net/documents/TAP/">IVOA VOSpace</a> 
service that uses storage-inventory as the back end storage mechanism.

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
