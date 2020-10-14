--
-- view
--
create or replace view {SCHEMA}cube_by_keyword as 
  select effective_date, max(product_id) as product_id, max(product_name) as product_name,
  channel, mmp, campaign_id, adset_id, ad_id, platform, event_name,
  daily_cohort, weekly_cohort, event_day, event_week, event_month,
  sum(cohort_events) as cohort_events, sum(cohort_revenue) as cohort_revenue,
  sum(time_series_events) as time_series_events, sum(time_series_revenue) as time_series_revenue,
  sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks, sum(reach) as reach,
  max(campaign_name) as campaign_name, max(campaign_tag) as campaign_tag, max(campaign_subtag) as campaign_subtag,
  max(adset_name) as adset_name, max(adset_tag) as adset_tag, max(adset_subtag) as adset_subtag,
  max(ad_name) as ad_name, max(ad_tag) as ad_tag, max(ad_subtag) as ad_subtag,
  max(campaign_status) as campaign_status, max(campaign_objective) as campaign_objective,
  max(adset_status) as adset_status,
  max(ad_type) as ad_type, max(ad_status) as ad_status, max(first_impression_date) as first_impression_date,
  max(creative_id) as creative_id, max(creative_name) as creative_name, max(creative_title) as creative_title,
  max(creative_body) as creative_body, max(creative_image_url) as creative_image_url,
  max(creative_thumbnail_url) as creative_thumbnail_url,
  min(channel_crystallized::int)::boolean as channel_crystallized, min(mmp_crystallized::int)::boolean as mmp_crystallized,
  min(crystallized::int)::boolean as crystallized
  from {SCHEMA}performance_cube_filtered
  where ad_id like 'flx-kw-%%'
  group by effective_date, channel, mmp, campaign_id, adset_id, ad_id, platform, event_name,
  daily_cohort, weekly_cohort, event_day, event_week, event_month