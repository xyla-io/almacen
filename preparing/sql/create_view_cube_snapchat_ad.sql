--
-- view
--
create or replace view {SCHEMA}cube_snapchat_ad as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select app_display_name as product_name, campaign_id, ad_squad_id as adset_id, ad_id, platform, start_time as effective_date, max(creative_id) as creative_id, 
sum(spend) as spend, sum(impressions) as impressions, sum(swipes) as clicks, sum(uniques) as reach, avg(frequency) as frequency, max(ad_status) as ad_status, 
max(campaign_name) as campaign_name, max(ad_squad_name) as adset_name, max(ad_name) as ad_name,
max(campaign_objective) as campaign_objective, max(ad_type) as ad_type,
min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_snapchat_ads
where start_time >= (select start_date from date_range) and start_time <= (select end_date from date_range)
group by app_display_name, campaign_id, ad_squad_id, ad_id, platform, effective_date