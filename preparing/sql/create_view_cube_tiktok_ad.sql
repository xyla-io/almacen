--
-- view
--
create or replace view {SCHEMA}cube_tiktok_ad as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select coalesce(product_name, '') as product_name
  , ad_campaign_id as campaign_id
  , ad_adgroup_id as adset_id
  , ad_ad_id as ad_id
  , coalesce(product_os, '') as platform
  , ad_stat_datetime as effective_date
  , sum(ad_stat_cost) as spend
  , sum(ad_show_cnt) as impressions
  , sum(ad_click_cnt) as clicks
  , max(ad_campaign_name) as campaign_name
  , max(ad_adgroup_name) as adset_name
  , max(ad_ad_name) as ad_name
  , max(ad_opt_status) as ad_status
  , max(ad_image_mode) as ad_type
  , min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_tiktok_ads
where ad_stat_datetime >= (select start_date from date_range)
  and ad_stat_datetime <= (select end_date from date_range)
group by coalesce(product_name, ''), ad_campaign_id, ad_adgroup_id, ad_ad_id, coalesce(product_os, ''), ad_stat_datetime