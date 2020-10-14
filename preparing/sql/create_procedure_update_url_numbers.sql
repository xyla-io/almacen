--
-- procedure
--
create or replace procedure {SCHEMA}update_url_numbers (table_name character varying, url_column character varying, number_column character varying, should_force bool)
as $$
declare
    q character varying;
begin
q := 'update {SCHEMA}' || table_name || ' ' ||
'set ' || number_column || ' = url_numbers.number ' ||
'from {SCHEMA}url_numbers ' ||
'where ';
if not should_force then
  q := q || table_name || '.' ||  number_column || ' = 0 and ';
end if;
q := q || table_name || '.' || url_column || ' = url_numbers.url;';
execute q;
end;
$$ language plpgsql;