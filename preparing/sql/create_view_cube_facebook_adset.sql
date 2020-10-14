--
-- view
--
create or replace view {SCHEMA}cube_facebook_adset as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select app_display_name as product_name, campaign_id, adset_id, decode(product_platform, 'web', product_platform, platform) as platform, date_start as effective_date, 
max(campaign_name) as campaign_name, max(adset_name) as adset_name,
SUM(spend) as spend, SUM(impressions) AS impressions, SUM(clicks) AS clicks, avg(frequency) as frequency,
min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_facebook_adsets
where date_start >= (select start_date from date_range) and date_start <= (select end_date from date_range)
group by app_display_name, campaign_id, adset_id, decode(product_platform, 'web', product_platform, platform), effective_date