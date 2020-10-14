--
-- view
--
create or replace view {SCHEMA}cube_tiktok_adgroup as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select coalesce(product_name, '') as product_name
  , adgroup_campaign_id as campaign_id
  , adgroup_adgroup_id as adset_id
  , coalesce(product_os, '') as platform
  , adgroup_stat_datetime as effective_date
  , sum(adgroup_stat_cost) as spend
  , sum(adgroup_show_cnt) as impressions
  , sum(adgroup_click_cnt) as clicks
  , max(adgroup_campaign_name) as campaign_name
  , max(adgroup_adgroup_name) as adset_name
  , max(adgroup_opt_status) as adset_status
  , min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_tiktok_adgroups
where adgroup_stat_datetime >= (select start_date from date_range)
  and adgroup_stat_datetime <= (select end_date from date_range)
group by coalesce(product_name, ''), adgroup_campaign_id, adgroup_adgroup_id, coalesce(product_os, ''), adgroup_stat_datetime