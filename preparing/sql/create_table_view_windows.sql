--
-- table
--
create table {SCHEMA}view_windows (
  views character varying(6138) not null,
  start_date_offset int,
  end_date_offset int,
  start_date_fixed date,
  end_date_fixed date,
  window_options character varying(32768),
  primary key (views)
)
distkey (views)
sortkey (views);

--
-- data
--
insert into {SCHEMA}view_windows (views, start_date_offset, end_date_offset)
values ('cube', -48, 0);