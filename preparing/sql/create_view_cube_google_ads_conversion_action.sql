--
-- view
--
create or replace view {SCHEMA}cube_google_ads_conversion_action as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select product_id, campaign_id, adset_id, ad_id, platform, event_name, daily_cohort, event_day, 
sum(cohort_events) as cohort_events, sum(cohort_revenue) as cohort_revenue, sum(cohort_events) as time_series_events, sum(cohort_revenue) as time_series_revenue,
min(crystallized::int)::bool as mmp_crystallized
from (
  select product_id, campaign_id, ad_group_id as adset_id, ad_group_ad_ad_id as ad_id, coalesce(product_os, 'web') as platform, segments_conversion_action_name as event_name,
  segments_date as daily_cohort, 0 as event_day,
  metrics_conversions as cohort_events, metrics_conversions_value as cohort_revenue,
  crystallized
  from {SCHEMA}fetch_google_ads_ad_conversion_actions
  where segments_date >= (select start_date from date_range) and segments_date <= (select end_date from date_range)
)
group by product_id, campaign_id, adset_id, ad_id, platform, event_name, daily_cohort, event_day