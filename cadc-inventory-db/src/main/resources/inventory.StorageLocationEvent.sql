
create table <schema>.StorageLocationEvent (
    uri varchar(512),
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null
);

create index sle_uri_index on <schema>.StorageLocationEvent(uri)
    where uri is not null;

create index sle_modified_index on <schema>.StorageLocationEvent(lastModified);

