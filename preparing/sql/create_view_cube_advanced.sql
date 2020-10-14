--
-- view
--
create or replace view {SCHEMA}cube_advanced as 
  select max(product_name) as product_name,
  daily_cohort, weekly_cohort, event_day, event_week, event_month,
  channel, platform,
  campaign_id, 
  max(campaign_name) as campaign_name, max(campaign_tag) as campaign_tag, max(campaign_subtag) as campaign_subtag,
  adset_id,
  max(adset_name) as adset_name, max(adset_tag) as adset_tag, max(adset_subtag) as adset_subtag,
  ad_id,
  max(ad_name) as ad_name, max(ad_tag) as ad_tag, max(ad_subtag) as ad_subtag,
  sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks, sum(reach) as reach,
  mmp as event_source, event_name,
  sum(cohort_events) as cohort_events, sum(cohort_revenue) as cohort_revenue,
  effective_date as time_series_date,
  sum(time_series_events) as time_series_events, sum(time_series_revenue) as time_series_revenue,
  max(campaign_status) as campaign_status, max(adset_status) as adset_status, max(ad_status) as ad_status,
  max(ad_type) as ad_type,
  max(creative_id) as creative_id, max(creative_name) as creative_name, max(creative_title) as creative_title,
  max(creative_body) as creative_body, max(creative_image_url) as creative_image_url,
  max(creative_thumbnail_url) as creative_thumbnail_url
  from  {SCHEMA}performance_cube_filtered
  group by daily_cohort, weekly_cohort, event_day, event_week, event_month,
  channel, platform, campaign_id, adset_id, ad_id, mmp, event_name, effective_date
