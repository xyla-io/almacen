select effective_date
  , channel
  , coalesce(mmp, '') as mmp
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
group by 1, 2, 3
order by 1, 2, 3;
