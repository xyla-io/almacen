--
-- view
--
create or replace view {SCHEMA}cube_tiktok_campaign as
with date_range as (
  select decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed) as start_date,
  decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed) as end_date
  from {SCHEMA}view_windows
  where views = 'cube'
)
select coalesce(product_name, '') as product_name
  , campaign_campaign_id as campaign_id
  , coalesce(product_os, '') as platform
  , campaign_stat_datetime as effective_date
  , sum(campaign_stat_cost) as spend
  , sum(campaign_show_cnt) as impressions
  , sum(campaign_click_cnt) as clicks
  , max(campaign_campaign_name) as campaign_name
  , max(campaign_opt_status) as campaign_status
  , max(campaign_objective_type) as campaign_objective
  , min(crystallized::int)::bool as crystallized
from {SCHEMA}fetch_tiktok_campaigns
where campaign_stat_datetime >= (select start_date from date_range) 
  and campaign_stat_datetime <= (select end_date from date_range)
group by coalesce(product_name, ''), campaign_campaign_id, coalesce(product_os, ''), campaign_stat_datetime