--
-- function
--
create or replace function {SCHEMA}try_float (s character varying) returns double precision immutable
as $$
  try:
    return float(s)
  except:
    return None
$$ language plpythonu;