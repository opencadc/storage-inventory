
create table <schema>.Artifact (
    uri varchar(512) not null,
    uriBucket char(5) not null,
    contentChecksum varchar(136) not null,
    contentLastModified timestamp not null,
    contentLength bigint not null,
    contentType varchar(128),
    contentEncoding varchar(128),

    siteLocations uuid[],
    storageLocation_storageID varchar(256),
    storageLocation_storageBucket varchar(8),

    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    id uuid not null primary key
);

create unique index uri_index on <schema>.Artifact(uri);

create index bucket_index on <schema>.Artifact(uriBucket);

create index a_modified_index on <schema>.Artifact(lastModified);
