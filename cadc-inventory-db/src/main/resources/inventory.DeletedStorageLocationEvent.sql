
create table <schema>.DeletedStorageLocationEvent (
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null
);

create index dsle_modified_index on <schema>.DeletedStorageLocationEvent(lastModified);

