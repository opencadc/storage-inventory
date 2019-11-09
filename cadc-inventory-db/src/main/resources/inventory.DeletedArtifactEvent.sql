
create table <schema>.DeletedArtifactEvent (
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null
);

create index dae_modified_index on <schema>.DeletedArtifactEvent(lastModified);
