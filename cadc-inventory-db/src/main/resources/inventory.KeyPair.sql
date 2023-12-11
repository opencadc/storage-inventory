
create table <schema>.KeyPair (
    name varchar(32) not null,
    publicKey bytea not null,
    privateKey bytea not null,

    id uuid not null primary key,
    lastModified timestamp not null,
    metaChecksum varchar(136) not null
);

create unique index kp_name_index on <schema>.KeyPair(name);
