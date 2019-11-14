
create table <schema>.StorageSite (
    resourceID varchar(512) not null,
    name varchar(32) not null,
    
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null
);

create unique index resourceid_index on <schema>.StorageSite(resourceID);

create index ss_modified_index on <schema>.StorageSite(lastModified);
