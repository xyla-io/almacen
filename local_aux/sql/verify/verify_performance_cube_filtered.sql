select effective_date
  , channel
  , coalesce(mmp, '') as mmp
  , coalesce(campaign_id, '') as campaign_id
  , coalesce(adset_id, '') as adset_id
  , coalesce(ad_id, '') as ad_id
  , daily_cohort
  , event_day
  , event_week
  , event_month
  , coalesce(event_name, '') as event_name
  , round(sum(spend), 2) as sum_spend
  , round(sum(cohort_events), 2) as sum_cohort_events
  , round(sum(cohort_revenue), 2) as sum_cohort_revenue
  , round(sum(time_series_events), 2) as sum_time_series_events
  , round(sum(time_series_revenue), 2) as sum_time_series_revenue
from {SCHEMA}.performance_cube_filtered
where effective_date >= dateadd(day, -6, current_date)
  and effective_date <= dateadd(day, -1, current_date)
  and daily_cohort >= dateadd(day, -6, current_date)
  and daily_cohort <= dateadd(day, -1, current_date)
  and event_day is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
having (coalesce(sum_spend, 0) != 0
  or coalesce(sum_cohort_events, 0) != 0
  or coalesce(sum_cohort_revenue, 0) != 0
  or coalesce(sum_time_series_events, 0) != 0
  or coalesce(sum_time_series_revenue, 0) != 0)
order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11;
