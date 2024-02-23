
alter table <schema>.Node 
    add column bytesUsed bigint,
    add column storageBucket varchar(5),
    add column delta bigint
;

create unique index node_storageID on <schema>.Node(storageID);