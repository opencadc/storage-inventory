
-- cadc-storage-adapter: StorageMetadata
create table <schema>.StorageMetadata (
    storageLocation_storageBucket varchar(512) not null,    
    storageLocation_storageID varchar(512) not null,

    artifactURI varchar(512) not null,
    contentChecksum varchar(136) not null,
    contentLastModified timestamp not null,
    contentLength bigint not null
);

-- uniqueness, sufficient for ordered iterator only
create unique index sm_storage_index on <schema>.StorageMetadata 
    (storageLocation_storageBucket,storageLocation_storageID);

-- uniqueness, maybe useful for diagnostics
create unique index sm_artifact_uri on <schema>.StorageMetadata (artifactURI);
