select channel
  , sum(spend) as spend
  , sum(cohort_events) as cohort_events
  , sum(time_series_events) as time_series_events
from {SCHEMA}.performance_cube_unfiltered
where effective_date between dateadd(day, -6, current_date) and dateadd(day, -1, current_date)
group by 1
order by 1