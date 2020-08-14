
-- default value just to allow upgrade to proceed: minoc will set with configured value
alter table <schema>.StorageSite
    add column allowRead boolean not null default false,
    add column allowWrite boolean not null default false;
