
create unique index storage_index on <schema>.Artifact(storageLocation_storageBucket,storageLocation_storageID)
    where storageLocation_storageID is not null;