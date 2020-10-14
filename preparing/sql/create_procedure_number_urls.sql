--
-- procedure
--
create or replace procedure {SCHEMA}number_urls (table_name character varying, url_column character varying, number_column character varying)
as $$
declare
    q character varying := 'insert into {SCHEMA}url_numbers (url) ';
begin
q := q || '(select url from (' ||
'(select distinct ' || url_column || ' as url from {SCHEMA}' || table_name;
if number_column is not null then
  q := q || ' where ' || number_column || ' = 0';
end if;
q := q || ') left join {SCHEMA}url_numbers using (url)) where number is null);';
execute q;
end;
$$ language plpgsql;