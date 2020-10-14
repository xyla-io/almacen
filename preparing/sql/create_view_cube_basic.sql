--
-- view
--
create or replace view {SCHEMA}cube_basic as 
  select max(product_name) as product_name,
  daily_cohort, weekly_cohort, event_day, event_week, event_month,
  channel, platform,
  campaign_id, 
  max(campaign_name) as campaign_name, max(campaign_tag) as campaign_tag, max(campaign_subtag) as campaign_subtag,
  sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks, sum(reach) as reach,
  event_name,
  sum(cohort_events) as cohort_events, sum(cohort_revenue) as cohort_revenue
  from  {SCHEMA}performance_cube_filtered
  group by daily_cohort, weekly_cohort, event_day, event_week, event_month,
  channel, platform, campaign_id, event_name
