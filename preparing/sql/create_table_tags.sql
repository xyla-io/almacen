--
-- table
--
create table {SCHEMA}tags (
  url character varying(256) not null default '',
  number bigint not null default 0,
  key character varying(64) not null,
  value character varying(256),
  flags bigint not null default 0,
  set character varying(64) not null default '',
  batch character varying(256),
  modified datetime not null default current_timestamp,
  primary key (number, key, set, url)
)
distkey (key)
sortkey (number, key);
