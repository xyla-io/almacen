--
-- table
--
create table {SCHEMA}track_task_history (
  id char(32) not null,
  creation_time datetime not null default current_timestamp,
  modification_time datetime not null default current_timestamp,
  status varchar(63) not null,
  message varchar(2046) default null,
  task_identifier varchar(2046) not null,
  task_name varchar(2046) not null,
  target_start_time datetime default null,
  target_end_time datetime default null,
  host varchar(2046) not null,
  primary key (id)
)
distkey (task_identifier)
sortkey (modification_time, task_identifier);
