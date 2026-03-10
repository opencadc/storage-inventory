
create table <schema>.DeletedStorageLocationEvent (
    uri varchar(512),
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null
);

create index dsle_uri_index on <schema>.DeletedStorageLocationEvent(uri)
    where uri is not null;

create index dsle_modified_index on <schema>.DeletedStorageLocationEvent(lastModified);

