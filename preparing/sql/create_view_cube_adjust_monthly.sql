--
-- view
--
create or replace view {SCHEMA}cube_adjust_monthly as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select channel, campaign_id, adset_id, ad_id, platform, event_name, daily_cohort, 
null::int as event_day, null::int as event_week, event_month,
sum(cohort_events) as cohort_events, sum(cohort_revenue) as cohort_revenue, sum(time_series_events) as time_series_events, sum(time_series_revenue) as time_series_revenue,
dateadd(day, event_month * 30, daily_cohort) as effective_date,
'["' || country || '", "' || region || '"]' as dynamic_segments,
'[' || sum(converted_users) || ',' || sum(first_events) || ']' as dynamic_metrics,
min(mmp_crystallized::int)::bool as mmp_crystallized
from (
  select {SCHEMA}adjust_network_channel(network) as channel, campaign_id, adgroup_id as adset_id, creative_id as ad_id, 
  os_name as platform, 'install' as event_name, country, region,
  cohort as daily_cohort, 0 as event_month, cohort_size as cohort_events, 0 as cohort_revenue, cohort_size as time_series_events, 0 as time_series_revenue,
  cohort_size as converted_users, 0 as first_events,
  crystallized as mmp_crystallized
  from {SCHEMA}fetch_adjust_cohorts_measures_monthly
  where month >= (select start_date from date_range) and month <= (select end_date from date_range) and month = cohort
  union all
  select {SCHEMA}adjust_network_channel(network) as channel, campaign_id, 
  adgroup_id as adset_id, creative_id as ad_id, os_name as platform, event_name, country, region,
  cohort as daily_cohort, datediff(day, cohort, month) / 30 as event_month, events as cohort_events, revenue as cohort_revenue, 0 as time_series_events, 0 as time_series_revenue,
  converted_users, 0 as first_events,
  crystallized as mmp_crystallized
  from {SCHEMA}fetch_adjust_cohorts_measures_monthly
  where month >= (select start_date from date_range) and month <= (select end_date from date_range)
  union all
  select {SCHEMA}adjust_network_channel(network) as channel, campaign_id, 
  adgroup_id as adset_id, creative_id as ad_id, os_name as platform, event_name, country, region, "date" as daily_cohort,
  0 as event_month, 0  as cohort_events, 0 as cohort_revenue, 
  events as time_series_events, revenue as time_series_revenue, 
  0 as converted_users, first_events,
  crystallized as mmp_crystallized
  from {SCHEMA}fetch_adjust_events
  where "date" >= (select start_date from date_range) and "date" <= (select end_date from date_range)
)
group by channel, campaign_id, adset_id, ad_id, platform, event_name, daily_cohort, event_month, dynamic_segments
