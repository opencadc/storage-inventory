
alter table <schema>.Artifact
    rename column storageLocation to storageLocation_storageID;

alter table <schema>.Artifact
    add column storageLocation_storageBucket varchar(8);
