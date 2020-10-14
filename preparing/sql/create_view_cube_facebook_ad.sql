--
-- view
--
create or replace view {SCHEMA}cube_facebook_ad as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select app_display_name as product_name, campaign_id, adset_id, ad_id, decode(product_platform, 'web', product_platform, platform) as platform, date_start as effective_date, 
max(campaign_name) as campaign_name, max(adset_name) as adset_name, max(ad_name) as ad_name,
max(creative_id) as creative_id, max(creative_name) as creative_name, max(creative_title) as creative_title, max(creative_body) as creative_body, max(creative_image_url) as creative_image_url, max(creative_thumbnail_url) as creative_thumbnail_url, sum(spend) as spend, sum(impressions) as impressions, 
sum(clicks) as clicks, avg(relevance_score) as relevance_score, sum(reach) as reach, 
avg(frequency) as frequency, null as ad_status, min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_facebook_ads
where date_start >= (select start_date from date_range) and date_start <= (select end_date from date_range)
group by app_display_name, campaign_id, adset_id, ad_id, decode(product_platform, 'web', product_platform, platform), effective_date