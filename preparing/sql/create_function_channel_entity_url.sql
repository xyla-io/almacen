--
-- function
--
create or replace function {SCHEMA}channel_entity_url(character varying, character varying, character varying) returns character varying
immutable
as $$
  select 'channel_entity://' ||
  case $1
  when 'Apple' then 'apple_search_ads'
  when 'Google' then 'google_ads'
  when 'Snapchat' then 'snapchat'
  when 'TikTok' then 'tiktok'
  when 'Facebook' then 'facebook'
  else $1
  end
  || '/' ||
  case $2
  when 'adset' then 'adgroup'
  when 'adsquad' then 'adgroup'
  when 'creative_set' then 'ad'
  else $2
  end
  || '/' || $3
$$ language sql;