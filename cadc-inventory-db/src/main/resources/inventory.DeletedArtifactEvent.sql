
create table <schema>.DeletedArtifactEvent (
    uri varchar(512),
    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null
);

create index dae_uri_index on <schema>.DeletedArtifactEvent(uri)
    where uri is not null;

create index dae_modified_index on <schema>.DeletedArtifactEvent(lastModified);
