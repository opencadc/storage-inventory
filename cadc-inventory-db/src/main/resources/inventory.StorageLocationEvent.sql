
create table <schema>.StorageLocationEvent (
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null
);

create index sle_modified_index on <schema>.StorageLocationEvent(lastModified);

