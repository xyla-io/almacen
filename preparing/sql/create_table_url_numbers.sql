--
-- table
--
create table {SCHEMA}url_numbers (
  url character varying(256),
  number bigint identity(1,1),
  primary key (url)
)
distkey (url)
sortkey (url);
