--
-- view
--
create or replace view {SCHEMA}cube_apple_search_ads_campaign as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select app_display_name as product_name, campaignId AS campaign_id, platform, "date" as effective_date, 
SUM(localSpend) as spend, SUM(impressions) AS impressions, SUM(taps) AS clicks,max(campaignname) as campaign_name, max(campaignStatus) as campaign_status, 
min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_apple_search_ads_campaigns
where "date" >= (select start_date from date_range) and "date" <= (select end_date from date_range)
group by app_display_name, campaign_id, platform, effective_date