-- add uri columns for events

alter table <schema>.DeletedArtifactEvent add column uri varchar(512);

alter table <schema>.DeletedStorageLocationEvent add column uri varchar(512);

alter table <schema>.StorageLocationEvent add column uri varchar(512);

create index dae_uri_index on <schema>.DeletedArtifactEvent(uri)
    where uri is not null;

create index dsle_uri_index on <schema>.DeletedStorageLocationEvent(uri)
    where uri is not null;

create index sle_uri_index on <schema>.StorageLocationEvent(uri)
    where uri is not null;
