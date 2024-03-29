-- delete key columns for keys from tables in the caom2 schema
delete from tap_schema.key_columns11 where
key_id in (select key_id from tap_schema.keys11 where
    from_table in (select table_name from tap_schema.tables11 where schema_name = 'inventory')
    or
    target_table in (select table_name from tap_schema.tables11 where schema_name = 'inventory')
)
;

-- delete keys from tables in the ivoa schema
delete from tap_schema.keys11 where
from_table in (select table_name from tap_schema.tables11 where schema_name = 'inventory')
or
target_table in (select table_name from tap_schema.tables11 where schema_name = 'inventory')
;

-- delete columns from tables in the ivoa schema
delete from tap_schema.columns11 where table_name in
(select table_name from tap_schema.tables11 where schema_name = 'inventory')
;

-- delete tables
delete from tap_schema.tables11 where schema_name = 'inventory'
;

-- delete the schema
delete from tap_schema.schemas11 where schema_name = 'inventory'
;

insert into tap_schema.schemas11 (schema_name,description) values
('inventory', 'tables and views used in the inventory system')
;

-- index start at 10
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,table_index) values
( 'inventory', 'inventory.Artifact', 'table', 'global: all artifacts; storage site: artifacts with local file', 10 ),
( 'inventory', 'inventory.StorageLocationEvent', 'table', 'Table of new storage location events', 11 ),
( 'inventory', 'inventory.StorageSite', 'table', 'Table of storage sites', 12 ),
( 'inventory', 'inventory.DeletedArtifactEvent', 'table', 'Table of deleted artifact events', 13 ),
( 'inventory', 'inventory.DeletedStorageLocationEvent', 'table', 'Table of deleted storage location events', 14 ),
( 'inventory', 'inventory.HarvestState', 'table', 'fenwick artifact-sync state', 15)
;

-- views
insert into tap_schema.tables11 (schema_name,table_name,table_type,description,table_index) values
( 'inventory', 'inventory.PendingArtifact', 'view', 'storage site: view of artifacts that are pending file-sync', 20)
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.Artifact', 'uri', 'si:Artifact.uri', 'URI for this file', NULL, 'char','512*', 'uri', 1, 1, 1, 1 ),
( 'inventory.Artifact', 'uriBucket', 'si:Artifact.uriBucket', 'URI for the storage bucket', NULL, 'char','5', NULL, 1, 1, 1, 2 ),
( 'inventory.Artifact', 'contentChecksum', 'si:Artifact.contentChecksum', 'checksum of the file content', NULL, 'char','136*', 'uri', 1, 0, 1, 3 ),
( 'inventory.Artifact', 'contentLastModified', 'si:Artifact.contentLastModified', 'timestamp of last modification of the file', NULL, 'char','23*','timestamp', 1, 0, 1, 4 ),
( 'inventory.Artifact', 'contentLength', 'si:Artifact.contentLength', 'size of the file', 'byte', 'long', NULL, NULL, 1, 0, 1, 5 ),
( 'inventory.Artifact', 'contentType', 'si:Artifact.contentType', 'format of the file', NULL, 'char','128*',NULL, 1, 0, 1, 6 ),
( 'inventory.Artifact', 'contentEncoding', 'si:Artifact.contentEncoding', 'encoding type of the file', NULL, 'char','128*',NULL, 1, 0, 1, 7 ),
( 'inventory.Artifact', 'lastModified', 'si:Entity.lastModified', 'timestamp of last modification of the metadata', NULL, 'char','23*','timestamp', 1, 1, 1, 8 ),
( 'inventory.Artifact', 'metaChecksum', 'si:Entity.metaChecksum', 'checksum of the file metadata', NULL, 'char','136*','uri', 1, 0, 1, 9 ),
( 'inventory.Artifact', 'id', 'si:Entity.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 10 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.PendingArtifact', 'uri', 'si:Artifact.uri', 'URI for this file', NULL, 'char','512*', 'uri', 1, 1, 1, 1 ),
( 'inventory.PendingArtifact', 'uriBucket', 'si:Artifact.uriBucket', 'URI for the storage bucket', NULL, 'char','5', NULL, 1, 1, 1, 2 ),
( 'inventory.PendingArtifact', 'contentChecksum', 'si:Artifact.contentChecksum', 'checksum of the file content', NULL, 'char','136*', 'uri', 1, 0, 1, 3 ),
( 'inventory.PendingArtifact', 'contentLastModified', 'si:Artifact.contentLastModified', 'timestamp of last modification of the file', NULL, 'char','23*','timestamp', 1, 0, 1, 4 ),
( 'inventory.PendingArtifact', 'contentLength', 'si:Artifact.contentLength', 'size of the file', 'byte', 'long', NULL, NULL, 1, 0, 1, 5 ),
( 'inventory.PendingArtifact', 'contentType', 'si:Artifact.contentType', 'format of the file', NULL, 'char','128*',NULL, 1, 0, 1, 6 ),
( 'inventory.PendingArtifact', 'contentEncoding', 'si:Artifact.contentEncoding', 'encoding type of the file', NULL, 'char','128*',NULL, 1, 0, 1, 7 ),
( 'inventory.PendingArtifact', 'lastModified', 'si:Entity.lastModified', 'timestamp of last modification of the metadata', NULL, 'char','23*','timestamp', 1, 1, 1, 8 ),
( 'inventory.PendingArtifact', 'metaChecksum', 'si:Entity.metaChecksum', 'checksum of the file metadata', NULL, 'char','136*','uri', 1, 0, 1, 9 ),
( 'inventory.PendingArtifact', 'id', 'si:Entity.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 10 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.StorageLocationEvent', 'id', 'si:Entity.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 1 ),
( 'inventory.StorageLocationEvent', 'lastModified', 'si:Entity.lastModified', 'timestamp of the event', NULL, 'char','23*','timestamp', 1, 1, 1, 2 ),
( 'inventory.StorageLocationEvent', 'metaChecksum', 'si:Entity.metaChecksum', 'checksum of the file metadata', NULL, 'char','136*','uri', 1, 0, 1, 3 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.StorageSite', 'resourceID', 'si:StorageSite.resourceID', 'ID of the storage site (URI)', NULL, 'char','512*', 'uri', 1, 1, 1, 1 ),
( 'inventory.StorageSite', 'name', 'si:StorageSite.name', 'Name of the storage site', NULL, 'char','32*',NULL, 1, 0, 1, 2 ),
( 'inventory.StorageSite', 'id', 'si:StorageSite.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 3 ),
( 'inventory.StorageSite', 'lastModified', 'si:StorageSite.lastModified', 'timestamp of the file at location', NULL, 'char','23*','timestamp', 1, 1, 1, 4 ),
( 'inventory.StorageSite', 'metaChecksum', 'si:StorageSite.metaChecksum', 'checksum of the file at location', NULL, 'char','136*','uri', 1, 0, 1, 5 ),
( 'inventory.StorageSite', 'allowRead', 'si:StorageSite.allowRead', 'storage site allows HEAD and GET', NULL, 'boolean', NULL, NULL, 1, 0, 1, 6 ),
( 'inventory.StorageSite', 'allowWrite', 'si:StorageSite.allowWrite', 'storage site allows DELETE and PUT', NULL, 'boolean', NULL, NULL, 1, 0, 1, 7 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.DeletedArtifactEvent', 'id', 'si:Entity.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 1 ),
( 'inventory.DeletedArtifactEvent', 'lastModified', 'si:Entity.lastModified', 'timestamp of the event', NULL, 'char','23*','timestamp', 1, 1, 1, 2 ),
( 'inventory.DeletedArtifactEvent', 'metaChecksum', 'si:Entity.metaChecksum', 'checksum of the file metadata', NULL, 'char','136*','uri', 1, 0, 1, 3 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.DeletedStorageLocationEvent', 'id', 'si:Entity.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 1 ),
( 'inventory.DeletedStorageLocationEvent', 'lastModified', 'si:Entity.lastModified', 'timestamp of the event', NULL, 'char','23*','timestamp', 1, 1, 1, 2 ),
( 'inventory.DeletedStorageLocationEvent', 'metaChecksum', 'si:Entity.metaChecksum', 'checksum of the file metadata', NULL, 'char','136*','uri', 1, 0, 1, 3 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.HarvestState', 'name',             'si:HarvestState.name',             'classname of the harvested entity', NULL, 'char','64*', NULL, 1, 1, 1, 1 ),
( 'inventory.HarvestState', 'resourceID',       'si:HarvestState.resourceID',       'ID of the remote luskan (URI)', NULL, 'char','512*', 'uri', 1, 1, 1, 2 ),
( 'inventory.HarvestState', 'curLastModified',  'si:HarvestState.curLastModified',  'lastModified timestamp of the last entity harvested', NULL, 'char','23*','timestamp', 1, 1, 1, 3 ),
( 'inventory.HarvestState', 'curID',            'si:HarvestState`.curID',           'id of the last entity harvested', NULL, 'char','36','uuid', 1, 1, 1, 4 ),
( 'inventory.HarvestState', 'id',               'si:HarvestState`.id',              'primary key', NULL, 'char','36','uuid', 1, 1, 1, 5 ),
( 'inventory.HarvestState', 'lastModified',     'si:HarvestState.lastModified',     'timestamp of the event', NULL, 'char','23*','timestamp', 1, 1, 1, 6 ),
( 'inventory.HarvestState', 'metaChecksum',     'si:HarvestState.metaChecksum',     'checksum of the file metadata', NULL, 'char','136*','uri', 1, 0, 1, 7 )

;
