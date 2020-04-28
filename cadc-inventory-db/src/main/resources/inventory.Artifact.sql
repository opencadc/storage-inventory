
create table <schema>.Artifact (
    uri varchar(512) not null,
    uriBucket char(5) not null,
    contentChecksum varchar(136) not null,
    contentLastModified timestamp not null,
    contentLength bigint not null,
    contentType varchar(128),
    contentEncoding varchar(128),

    siteLocations uuid[],
    storageLocation_storageID varchar(512),
    storageLocation_storageBucket varchar(512),

    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    id uuid not null primary key
);

create unique index uri_index on <schema>.Artifact(uri);

create index bucket_index on <schema>.Artifact(uriBucket);

--create index a_modified_index on <schema>.Artifact(lastModified);
create index a_stored_index on <schema>.Artifact(lastModified)
    where storageLocation_storageID is not null;

create index a_unstored_index on <schema>.Artifact(lastModified)
    where storageLocation_storageID is null;

create unique index storage_index on <schema>.Artifact(storageLocation_storageBucket,storageLocation_storageID)
    where storageLocation_storageID is not null;