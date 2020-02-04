
create table <schema>.ObsoleteStorageLocation (
    location_storageID varchar(512),
    location_storageBucket varchar(8),

    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    id uuid not null primary key
);

create unique index dsl_location_index 
    on <schema>.ObsoleteStorageLocation(location_storageBucket,location_storageID);

create index dsl_modified_index on <schema>.ObsoleteStorageLocation(lastModified);


