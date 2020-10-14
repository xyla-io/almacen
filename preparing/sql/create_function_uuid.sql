--
-- function
--
create or replace function {SCHEMA}uuid () returns character varying volatile
as $$
  import uuid
  return uuid.uuid4().hex
$$ language plpythonu;