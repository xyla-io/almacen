--
-- procedure
--
create or replace procedure {SCHEMA}update_all_url_numbers (should_force bool)
as $$
begin
call {SCHEMA}update_url_numbers('tags', 'url', 'number', should_force);
end;
$$ language plpgsql;