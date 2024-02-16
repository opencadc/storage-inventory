
create table <schema>.DeletedNodeEvent (
    -- type is immutable
    nodeType char(1) not null,

    -- support cleanup of obsolete artifacts
    storageID varchar(512),

    lastModified timestamp not null,
    metaChecksum varchar(136) not null,
    id uuid not null primary key
);



create index dne_lastmodified on <schema>.DeletedNodeEvent(lastModified);

