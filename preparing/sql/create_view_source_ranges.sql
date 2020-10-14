--
-- view
--
create or replace view {SCHEMA}source_ranges as
with appsflyer_range as (
  select min("event time")::date as start_date, max("event time")::date as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_appsflyer_custom_events
  union all
  select null as start_date, null as end_date, min("event time")::date as crystallized_start_date, max("event time")::date as crystallized_end_date
  from {SCHEMA}fetch_appsflyer_custom_events
  where crystallized
  union all
  select min("event time")::date as start_date, max("event time")::date as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_appsflyer_install_events
  union all
  select null as start_date, null as end_date, min("event time")::date as crystallized_start_date, max("event time")::date as crystallized_end_date
  from {SCHEMA}fetch_appsflyer_install_events
  where crystallized
  union all
  select min("event time")::date as start_date, max("event time")::date as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_appsflyer_purchase_events
  union all
  select null as start_date, null as end_date, min("event time")::date as crystallized_start_date, max("event time")::date as crystallized_end_date
  from {SCHEMA}fetch_appsflyer_purchase_events
  where crystallized
),
adjust_range as (
  select min("day") as start_date, max("day") as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_adjust_cohorts_measures_daily
  union all
  select null as start_date, null as end_date, min("day") as crystallized_start_date, max("day") as crystallized_end_date
  from {SCHEMA}fetch_adjust_cohorts_measures_daily
  where crystallized
)
select channel, entity, max(start_date) as start_date, min(end_date) as end_date, max(crystallized_start_date) as crystallized_start_date, min(crystallized_end_date) as crystallized_end_date
from (
  select 'Apple' as channel, 'adset' as entity, min("date") as start_date, max("date") as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_apple_search_ads_adgroups
  union all
  select 'Apple' as channel, 'adset' as entity, null as start_date, null as end_date, min("date") as crystallized_start_date, max("date") as crystallized_end_date
  from {SCHEMA}fetch_apple_search_ads_adgroups
  where crystallized
  union all
  select 'Apple' as channel, 'campaign' as entity, min("date") as start_date, max("date") as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_apple_search_ads_campaigns
  union all
  select 'Apple' as channel, 'campaign' as entity, null as start_date, null as end_date, min("date") as crystallized_start_date, max("date") as crystallized_end_date
  from {SCHEMA}fetch_apple_search_ads_campaigns
  where crystallized
  union all
  select 'Facebook' as channel, 'ad' as entity, min(date_start) as start_date, max(date_start) as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_facebook_ads
  union all
  select 'Facebook' as channel, 'ad' as entity, null as start_date, null as end_date, min(date_start) as crystallized_start_date, max(date_start) as crystallized_end_date
  from {SCHEMA}fetch_facebook_ads
  where crystallized
  union all
  select 'Facebook' as channel, 'campaign' as entity, min(date_start) as start_date, max(date_start) as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_facebook_campaigns
  union all
  select 'Facebook' as channel, 'campaign' as entity, null as start_date, null as end_date, min(date_start) as crystallized_start_date, max(date_start) as crystallized_end_date
  from {SCHEMA}fetch_facebook_campaigns
  where crystallized
  union all
  select 'Google' as channel, 'ad' as entity, min("date") as start_date, max("date") as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}scrape_google_uac_creatives
  union all
  select 'Google' as channel, 'ad' as entity, null as start_date, null as end_date, min("date") as crystallized_start_date, max("date") as crystallized_end_date
  from {SCHEMA}scrape_google_uac_creatives
  where crystallized
  union all
  select 'Snapchat' as channel, 'ad' as entity, min("start_time") as start_date, max("start_time") as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_snapchat_ads
  union all
  select 'Snapchat' as channel, 'ad' as entity, null as start_date, null as end_date, min("start_time") as crystallized_start_date, max("start_time") as crystallized_end_date
  from {SCHEMA}fetch_snapchat_ads
  where crystallized
  union all
  select 'Snapchat' as channel, 'campaign' as entity, min("start_time") as start_date, max("start_time") as end_date, null as crystallized_start_date, null as crystallized_end_date
  from {SCHEMA}fetch_snapchat_campaigns
  union all
  select 'Snapchat' as channel, 'campaign' as entity, null as start_date, null as end_date, min("start_time") as crystallized_start_date, max("start_time") as crystallized_end_date
  from {SCHEMA}fetch_snapchat_campaigns
  where crystallized
  union all
  select 'AppsFlyer' as channel, 'ad' as entity, min(start_date) as start_date, max(end_date) as end_date, min(crystallized_start_date) as crystallized_start_date, dateadd(day, -1, max(crystallized_end_date))::date as crystallized_end_date
  from appsflyer_range
  union all
  select 'AppsFlyer' as channel, 'adset' as entity, min(start_date) as start_date, max(end_date) as end_date, min(crystallized_start_date) as crystallized_start_date, dateadd(day, -1, max(crystallized_end_date))::date as crystallized_end_date
  from appsflyer_range
  union all
  select 'AppsFlyer' as channel, 'campaign' as entity, min(start_date) as start_date, max(end_date) as end_date, min(crystallized_start_date) as crystallized_start_date, dateadd(day, -1, max(crystallized_end_date))::date as crystallized_end_date
  from appsflyer_range
  union all
  select 'Adjust' as channel, 'ad' as entity, min(start_date) as start_date, max(end_date) as end_date, min(crystallized_start_date) as crystallized_start_date, max(crystallized_end_date) as crystallized_end_date
  from adjust_range
  union all
  select 'Adjust' as channel, 'adset' as entity, min(start_date) as start_date, max(end_date) as end_date, min(crystallized_start_date) as crystallized_start_date, max(crystallized_end_date) as crystallized_end_date
  from adjust_range
  union all
  select 'Adjust' as channel, 'campaign' as entity, min(start_date) as start_date, max(end_date) as end_date, min(crystallized_start_date) as crystallized_start_date, max(crystallized_end_date) as crystallized_end_date
  from adjust_range
)
group by channel, entity