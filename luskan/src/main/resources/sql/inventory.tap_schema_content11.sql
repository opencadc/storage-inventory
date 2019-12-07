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
( 'inventory', 'inventory.Artifact', 'table', 'Artifact table', 10 ),
( 'inventory', 'inventory.DeletedArtifactEvent', 'table', 'Table of deleted artifact events', 11 ),
( 'inventory', 'inventory.DeletedStorageLocationEvent', 'table', 'Table of deleted location events', 12 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.Artifact', 'uri', 'si:Artifact.uri', 'URI for this file', NULL, 'char','512*', NULL, 1, 1, 1, 1 ),
( 'inventory.Artifact', 'uriBucket', 'si:Artifact.uriBucket', 'URI for the storage bucket', NULL, 'char','5', NULL, 1, 1, 1, 2 ),
( 'inventory.Artifact', 'contentChecksum', 'si:Artifact.contentChecksum', 'checksum of the file content', NULL, 'char','136*', NULL, 1, 0, 1, 3 ),
( 'inventory.Artifact', 'contentLastModified', 'si:Artifact.contentLastModified', 'timestamp of last modification of the file', NULL, 'char','23*','timestamp', 1, 0, 1, 4 ),
( 'inventory.Artifact', 'contentLength', 'si:Artifact.contentLength', 'size of the file', 'byte', 'long', NULL, NULL, 1, 0, 1, 5 ),
( 'inventory.Artifact', 'contentType', 'si:Artifact.contentType', 'format of the file', NULL, 'char','128*',NULL, 1, 0, 1, 6 ),
( 'inventory.Artifact', 'contentEncoding', 'si:Artifact.contentEncoding', 'encoding type of the file', NULL, 'char','128*',NULL, 1, 0, 1, 7 ),
( 'inventory.Artifact', 'lastModified', 'si:Entity.lastModified', 'timestamp of last modification of the metadata', NULL, 'char','23*','timestamp', 1, 1, 1, 8 ),
( 'inventory.Artifact', 'metaChecksum', 'si:Entity.metaChecksum', 'checksum of the file metadata', NULL, 'char','136*',NULL, 1, 0, 1, 9 ),
( 'inventory.Artifact', 'id', 'si:Entity.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 10 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.DeletedArtifactEvent', 'id', 'si:Entity.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 1 ),
( 'inventory.DeletedArtifactEvent', 'lastModified', 'si:Entity.lastModified', 'timestamp of the event', NULL, 'char','23*','timestamp', 1, 1, 1, 2 ),
( 'inventory.DeletedArtifactEvent', 'metaChecksum', 'si:Entity.metaChecksum', 'checksum of the file metadata', NULL, 'char','136*',NULL, 1, 0, 1, 3 )
;

insert into tap_schema.columns11 (table_name,column_name,utype,description,unit,datatype,arraysize,xtype,principal,indexed,std,column_index) values
( 'inventory.DeletedStorageLocationEvent', 'id', 'si:Entity.id', 'primary key', NULL, 'char','36','uuid', 1, 1, 1, 1 ),
( 'inventory.DeletedStorageLocationEvent', 'lastModified', 'si:Entity.lastModified', 'timestamp of the event', NULL, 'char','23*','timestamp', 1, 1, 1, 2 ),
( 'inventory.DeletedStorageLocationEvent', 'metaChecksum', 'si:Entity.metaChecksum', 'checksum of the file metadata', NULL, 'char','136*',NULL, 1, 0, 1, 3 )
;

