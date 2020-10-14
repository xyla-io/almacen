--
-- function
--
create or replace function {SCHEMA}try_int (s character varying) returns bigint immutable
as $$
  try:
    return int(s)
  except:
    return None
$$ language plpythonu;