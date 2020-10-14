--
-- function
--
create or replace function {SCHEMA}quote_json (s character varying) returns character varying immutable
as $$
  import json
  return json.dumps(s)
$$ language plpythonu;