--
-- procedure
--
create or replace procedure {SCHEMA}replace_standard_tags_view ()
as $$
declare
    q character varying(32768);
begin
execute 'drop view if exists {SCHEMA}standard_tag_columns;';
q := 'create view {SCHEMA}standard_tag_columns as' ||
' select url, max(number) as number' ||
coalesce((select listagg(distinct ', max(decode(key, ' || quote_literal(key) || ', value, null)) as ' || quote_ident(key)) within group (order by key) from {SCHEMA}tags where key != '' and set = 'standard'), '') ||
' from {SCHEMA}tags where key != '''' and set != ''standard'' group by url;';
execute q;
end;
$$ language plpgsql;
