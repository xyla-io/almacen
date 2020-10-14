--
-- table
--
create table {SCHEMA}maps (
  url character varying(256) not null,
  key_map character varying(8192),
  flags bigint not null default 0,
  set character varying(64) not null default '',
  batch character varying(256),
  modified datetime not null default current_timestamp,
  primary key (url, set)
)
distkey (url)
sortkey (url);
