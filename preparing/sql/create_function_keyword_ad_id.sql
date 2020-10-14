--
-- function
--
create or replace function {SCHEMA}keyword_ad_id (character varying, character varying, character varying)
  returns character varying
immutable
as $$
  select 'flx-kw-' || coalesce($1, '') || '-' || coalesce($2, '') || '-' || decode($3, null, '', '', '', func_sha1($3))
$$ language sql;
