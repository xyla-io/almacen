--
-- view
--
create or replace view {SCHEMA}cube_google_ads_campaign as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select product_name, campaign_id, coalesce(product_os, 'web') as platform, segments_date as effective_date, sum(cost) as spend, sum(metrics_impressions) as impressions, sum(metrics_clicks) as clicks, 
max(campaign_name) as campaign_name, max(campaign_status) as campaign_status, min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_google_ads_campaigns
where segments_date >= (select start_date from date_range) and segments_date <= (select end_date from date_range)
group by product_name, campaign_id, coalesce(product_os, 'web'), effective_date