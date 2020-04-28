create table <schema>.HarvestState (
    name varchar(64),
    resourceID varchar(128),
    curLastModified timestamp,

    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    id uuid not null primary key
);

create unique index hs_source_index 
    on <schema>.HarvestState(name, resourceID);
