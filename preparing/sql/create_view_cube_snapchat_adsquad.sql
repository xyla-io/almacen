--
-- view
--
create or replace view {SCHEMA}cube_snapchat_adsquad as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select app_display_name as product_name, campaign_id, ad_squad_id as adset_id, platform, start_time as effective_date, 
sum(spend) as spend, sum(impressions) as impressions, sum(swipes) as clicks, sum(uniques) as reach, avg(frequency) as frequency,
max(campaign_name) as campaign_name, max(ad_squad_name) as adset_name, max(ad_squad_status) as adset_status, min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_snapchat_adsquads
where start_time >= (select start_date from date_range) and start_time <= (select end_date from date_range)
group by app_display_name, campaign_id, ad_squad_id, platform, effective_date