--
-- materialized view
--
create materialized view {SCHEMA}standard_tags
backup NO
distkey (url)
sortkey (number) as
select url,
  max(number) as number
  , '{{' || 
  listagg('"' || replace(replace(replace(replace(replace(replace(replace(key, '\\', '\\\\'), '"', '\\"'), '\b', '\\b'), '\f', '\\f'), '\n', '\\n'), '\r', '\\r'), '\t', '\\t') || 
  '":"' || replace(replace(replace(replace(replace(replace(replace(value, '\\', '\\\\'), '"', '\\"'), '\b', '\\b'), '\f', '\\f'), '\n', '\\n'), '\r', '\\r'), '\t', '\\t') || '"', ',') within group (order by key) 
  || '}}' as tags
from {SCHEMA}tags
where key != ''
group by url;