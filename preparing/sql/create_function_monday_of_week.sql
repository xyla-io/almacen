--
-- function
--
create or replace function {SCHEMA}monday_of_week (date)
  returns date
immutable
as $$
  select CASE DATE_PART(dayofweek, $1)::int
    WHEN 0 THEN DATEADD(day, -6, $1)::date
    ELSE DATEADD(day, -DATE_PART(dayofweek, $1)::int + 1, $1)::date
    END
$$ language sql;
