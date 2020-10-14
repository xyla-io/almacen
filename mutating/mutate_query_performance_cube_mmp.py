import tasks

from data_layer import SQLQuery
from .mutate_query_base import LiteralQuery, SelectQuery, DeleteQuery, CustomSelectQuery, CustomUnionQuery, JSONStringQuery, JSONArrayQuery
from .mutate_query_performance_cube import PerformanceCubeRowType, PerformanceCubeEntity, PerformanceCubeCohortGranularity, PerformanceCubeTimeGranularity, PerformanceCubeSelectQuery
from typing import List, Dict, OrderedDict as OrderedDictType, Optional
from collections import OrderedDict
from models import PerformanceCubeCohortAnchor, PerformanceCubeTimeline, PerformanceCubeAdAlias

class PerformanceCubeMMPDeleteQuery(DeleteQuery):
  @property
  def condition_queries(self) -> List[SQLQuery]:
    queries = [
      SQLQuery(f'(entity_granularity = %s and {e.value}_id = %s)', (e.value, ''))
      for e in PerformanceCubeEntity.campaign.lower(inclusive=False)
    ]
    return [
      SQLQuery(
        query='\nor '.join(q.query for q in queries),
        substitution_parameters=tuple(p for q in queries for p in q.substitution_parameters)
      ),
    ]

class AppsFlyerCubeQuery(SelectQuery):
  schema_prefix: str
  source_table: str
  timeline: PerformanceCubeTimeline
  cohort: PerformanceCubeCohortAnchor
  ad_alias: Optional[PerformanceCubeAdAlias]

  def __init__(self, schema_prefix: str, source_table: str, timeline: PerformanceCubeTimeline=PerformanceCubeTimeline.cohort, cohort: PerformanceCubeCohortAnchor=PerformanceCubeCohortAnchor.attribution, ad_alias: Optional[PerformanceCubeAdAlias]=None):
    self.schema_prefix = schema_prefix
    self.source_table = source_table
    self.timeline = timeline
    self.cohort = cohort
    self.ad_alias = ad_alias
    super().__init__()

  @property
  def cohort_column(self) -> str:
    if self.cohort is PerformanceCubeCohortAnchor.attribution:
      return 'attributed touch time'
    elif self.cohort is PerformanceCubeCohortAnchor.install:
      return 'install time'

  @property
  def time_series_column(self) -> str:
    return 'event time'

  @property
  def full_source_name(self) -> str:
    return f'{self.schema_prefix}{self.source_table}'

  @property
  def source_columns(self) -> List[str]:
    return [
      'product_id',
      'channel',
      'campaign_id',
      'adset_id',
      'ad_id',
      'platform',
      'event_name',
      'daily_cohort',
      'event_day',
      'cohort_events',
      'cohort_revenue',
      'time_series_events',
      'time_series_revenue',
      'mmp_crystallized',
    ]

  @property
  def ad_id_expression(self) -> SQLQuery:
    if self.ad_alias is None:
      return SQLQuery('coalesce("ad id", %s)', ('',))
    elif self.ad_alias is PerformanceCubeAdAlias.keyword:
      return SQLQuery(f'{self.schema_prefix}keyword_ad_id("campaign id", "adset id", keywords)')

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    events_query = SQLQuery('count(*)')
    revenue_query = SQLQuery('sum("event revenue usd")')
    zero_query = SQLQuery('0')
    return {
      'product_id': SQLQuery('app_id'),
      'channel': SQLQuery(f'{self.schema_prefix}appsflyer_media_source_channel("media source")'),
      'campaign_id': SQLQuery('coalesce("campaign id", %s)', ('',)),
      'adset_id': SQLQuery('coalesce("adset id", %s)', ('',)),
      'ad_id': self.ad_id_expression,
      'event_name': SQLQuery('"event name"'),
      'daily_cohort': SQLQuery(f'"{self.cohort_column if self.timeline is PerformanceCubeTimeline.cohort else self.time_series_column}"::date'),
      'event_day': SQLQuery(f'datediff(second, "{self.cohort_column}", "event time") / 86400') if self.timeline is PerformanceCubeTimeline.cohort else zero_query,
      'cohort_events': events_query if self.timeline is PerformanceCubeTimeline.cohort else zero_query,
      'cohort_revenue': revenue_query if self.timeline is PerformanceCubeTimeline.cohort else zero_query,
      'time_series_events': events_query if self.timeline is PerformanceCubeTimeline.time_series else zero_query,
      'time_series_revenue': revenue_query if self.timeline is PerformanceCubeTimeline.time_series else zero_query,
      'mmp_crystallized': SQLQuery('min(crystallized::int)::bool'),
    }
  
  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('"event time" >= (select event_start_date from date_range)'),
      SQLQuery('"event time" < (select event_end_date from date_range)'),
    ]

  @property
  def group_by_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('app_id'),
      SQLQuery(f'{self.schema_prefix}appsflyer_media_source_channel("media source")'),
      SQLQuery('campaign_id'),
      SQLQuery('adset_id'),
      SQLQuery('ad_id'),
      SQLQuery('platform'),
      SQLQuery('event_name'),
      SQLQuery('daily_cohort'),
      SQLQuery('event_day'),
    ]

  @property
  def quote_columns(self) -> bool:
    return False

class AppsFlyerDataLockerCubeQuery(AppsFlyerCubeQuery):
  @property
  def cohort_column(self) -> str:
    if self.cohort is PerformanceCubeCohortAnchor.attribution:
      return 'attributed_touch_date'
    elif self.cohort is PerformanceCubeCohortAnchor.install:
      return 'install_date'

  @property
  def time_series_column(self) -> str:
    return 'event_date'

  @property
  def source_columns(self) -> List[str]:
    return [
      'app_id',
      'channel',
      'campaign_id',
      'adset_id',
      'ad_id',
      'platform',
      'event_name',
      'cohort',
      'event_day',
      'events',
      'revenue',
      'date_events',
      'date_revenue',
      'crystallized',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    events_query = SQLQuery('sum(events)')
    revenue_query = SQLQuery('sum(revenue)')
    zero_query = SQLQuery('0')
    return {
      'channel': SQLQuery(f'{self.schema_prefix}appsflyer_media_source_channel(media_source)'),
        **({
          'ad_id': SQLQuery(f'{self.schema_prefix}keyword_ad_id(campaign_id, adset_id, keyword)')
        } if self.ad_alias is PerformanceCubeAdAlias.keyword else {}),
      'cohort': SQLQuery(self.cohort_column if self.timeline is PerformanceCubeTimeline.cohort else self.time_series_column),
      **({
        'event_day': zero_query if self.timeline is PerformanceCubeTimeline.time_series else SQLQuery(f'datediff(day, {self.cohort_column}, event_date)'),
      } if self.timeline is PerformanceCubeTimeline.time_series or self.cohort is PerformanceCubeCohortAnchor.install else {}),
      'events': events_query if self.timeline is PerformanceCubeTimeline.cohort else zero_query,
      'revenue': revenue_query if self.timeline is PerformanceCubeTimeline.cohort else zero_query,
      'date_events': events_query if self.timeline is PerformanceCubeTimeline.time_series else zero_query,
      'date_revenue': revenue_query if self.timeline is PerformanceCubeTimeline.time_series else zero_query,
      'crystallized': SQLQuery(f'min({"effective_date_crystallized" if self.timeline is PerformanceCubeTimeline.cohort and self.cohort is PerformanceCubeCohortAnchor.attribution else "event_date_crystallized"}::int)::bool'),
    }

  @property
  def condition_queries(self) -> List[SQLQuery]:
    date_column = 'effective_date' if self.timeline is PerformanceCubeTimeline.cohort and self.cohort is PerformanceCubeCohortAnchor.attribution else 'event_date'
    return [
      SQLQuery(f'{date_column} >= (select start_date from date_range)'),
      SQLQuery(f'{date_column} <= (select end_date from date_range)'),
      *([
        SQLQuery('cohort is not null'),
        *([
            SQLQuery('event_day is not null')
          ] if self.cohort is PerformanceCubeCohortAnchor.attribution else []),
      ] if self.timeline is PerformanceCubeTimeline.cohort else [])
    ]

  @property
  def group_by_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('app_id'),
      SQLQuery(f'{self.schema_prefix}appsflyer_media_source_channel(media_source)'),
      SQLQuery('campaign_id'),
      SQLQuery('adset_id'),
      SQLQuery('keyword' if self.ad_alias is PerformanceCubeAdAlias.keyword else 'ad_id'),
      SQLQuery('platform'),
      SQLQuery('event_name'),
      SQLQuery('cohort'),
      SQLQuery('event_date') if self.timeline is PerformanceCubeTimeline.cohort and self.cohort is PerformanceCubeCohortAnchor.install else SQLQuery('event_day'),
    ]

class AppsFlyerDataLockerOrganicCubeQuery(AppsFlyerDataLockerCubeQuery):
  @property
  def source_columns(self) -> List[str]:
    return [
      'product_id',
      'platform',
      'event_name',
      'cohort',
      'event_day',
      'cohort_events',
      'cohort_revenue',
      'time_series_events',
      'time_series_revenue',
      'mmp_crystallized',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    events_query = SQLQuery('sum(events)')
    revenue_query = SQLQuery('sum(revenue)')
    zero_query = SQLQuery('0')
    return {
      'product_id': SQLQuery('app_id'),
      'cohort': SQLQuery(self.cohort_column if self.timeline is PerformanceCubeTimeline.cohort else self.time_series_column),
      'event_day': SQLQuery(f'datediff(day, {self.cohort_column}, event_date)') if self.timeline is PerformanceCubeTimeline.cohort else zero_query,
      'cohort_events': events_query if self.timeline is PerformanceCubeTimeline.cohort else zero_query,
      'cohort_revenue': revenue_query if self.timeline is PerformanceCubeTimeline.cohort else zero_query,
      'time_series_events': events_query if self.timeline is PerformanceCubeTimeline.time_series else zero_query,
      'time_series_revenue': revenue_query if self.timeline is PerformanceCubeTimeline.time_series else zero_query,
      'mmp_crystallized': SQLQuery(f'min({"effective_date_crystallized" if self.timeline is PerformanceCubeTimeline.cohort and self.cohort is PerformanceCubeCohortAnchor.attribution else "event_date_crystallized"}::int)::bool'),
    }

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('media_source = %s', ('organic',)),
      *super().condition_queries,
    ]

  @property
  def group_by_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('app_id'),
      SQLQuery('platform'),
      SQLQuery('event_name'),
      *([
        SQLQuery('install_date'),
      ] if self.timeline is PerformanceCubeTimeline.cohort else []),
      SQLQuery('event_date'),
    ]

class PerformanceCubeAppsFlyerBaseQuery(PerformanceCubeSelectQuery):
  @property
  def ad_alias(self) -> Optional[PerformanceCubeAdAlias]:
    return None

  @property
  def select_event_dates(self) -> bool:
    return True

  @property
  def appsflyer_query(self) -> SQLQuery:
    return CustomSelectQuery(
      source_columns=[
        'product_id',
        'channel',
        'campaign_id',
        'adset_id',
        'ad_id',
        'platform',
        'event_name',
        'daily_cohort',
        'event_day',
        'cohort_events',
        'cohort_revenue',
        'time_series_events',
        'time_series_revenue',
        'mmp_crystallized',
        'effective_date',
      ],
      source_column_expressions={
        'cohort_events': SQLQuery('sum(cohort_events)'),
        'cohort_revenue': SQLQuery('sum(cohort_revenue)'),
        'time_series_events': SQLQuery('sum(time_series_events)'),
        'time_series_revenue': SQLQuery('sum(time_series_revenue)'),
        'effective_date': SQLQuery('dateadd(day, event_day, daily_cohort)::date'),
        'mmp_crystallized': SQLQuery('min(mmp_crystallized::int)::bool'),
      },
      source_query=CustomUnionQuery(
        select_queries=[
          AppsFlyerCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_install_events',
            cohort=self.task.cohort_anchor,
            ad_alias=self.ad_alias
          ),
          AppsFlyerCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_purchase_events',
            cohort=self.task.cohort_anchor,
            ad_alias=self.ad_alias
          ),
          AppsFlyerCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_custom_events',
            cohort=self.task.cohort_anchor,
            ad_alias=self.ad_alias
          ),
          AppsFlyerCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_install_events',
            timeline=PerformanceCubeTimeline.time_series,
            cohort=self.task.cohort_anchor,
            ad_alias=self.ad_alias
          ),
          AppsFlyerCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_purchase_events',
            timeline=PerformanceCubeTimeline.time_series,
            cohort=self.task.cohort_anchor,
            ad_alias=self.ad_alias
          ),
          AppsFlyerCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_custom_events',
            timeline=PerformanceCubeTimeline.time_series,
            cohort=self.task.cohort_anchor,
            ad_alias=self.ad_alias
          ),
        ]
      ),
      condition_queries=[
        SQLQuery('effective_date >= (select start_date from date_range)'),
        SQLQuery('effective_date <= (select end_date from date_range)'),
      ],
      group_by_queries=[
        SQLQuery('product_id'),
        SQLQuery('channel'),
        SQLQuery('campaign_id'),
        SQLQuery('adset_id'),
        SQLQuery('ad_id'),
        SQLQuery('platform'),
        SQLQuery('event_name'),
        SQLQuery('daily_cohort'),
        SQLQuery('event_day'),
      ],
      quote_columns=False
    )

  @property
  def with_queries(self) -> OrderedDictType[str, any]:
    return OrderedDict([
      ('date_range', CustomSelectQuery(
        source_columns=[
          'start_date',
          'end_date',
          *([
            'event_start_date',
            'event_end_date',
          ] if self.select_event_dates else [])
        ],
        source_column_expressions={
          'event_start_date': SQLQuery('start_date'),
          'event_end_date': SQLQuery('dateadd(day, 2, end_date)'),
        } if self.select_event_dates else {},
        source_query=CustomSelectQuery(
          source_columns=[
            'start_date',
            'end_date',
          ],
          source_column_expressions={
            'start_date': SQLQuery('decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed)'),
            'end_date': SQLQuery('decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed)')
          },
          full_source_name=f'{self.task.schema_prefix}view_windows',
          condition_queries=[
            SQLQuery('views = %s', ('cube',)),
          ],
          quote_columns=False
        ),
        quote_columns=False
      )),
      (self.full_source_name, self.appsflyer_query)
    ])

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'mmp',
      'product_id', 
      'channel', 
      'campaign_id',
      'adset_id',
      'ad_id',
      'platform',
      'event_name',
      'daily_cohort',
      'weekly_cohort',
      'event_day',
      'event_week',
      'event_month',
      'cohort_events',
      'cohort_revenue',
      'time_series_events',
      'time_series_revenue',
      'mmp_crystallized',
      'crystallized',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'row_type': LiteralQuery(PerformanceCubeRowType.aggregation.value),
      'entity_granularity': LiteralQuery(PerformanceCubeEntity.ad.value),
      'cohort_granularity': LiteralQuery(PerformanceCubeCohortGranularity.daily.value),
      'time_granularity': LiteralQuery(PerformanceCubeTimeGranularity.day.value),
      'mmp': LiteralQuery('AppsFlyer'),
      'weekly_cohort': SQLQuery(f'{self.task.schema_prefix}monday_of_week(daily_cohort)'),
      'event_week': SQLQuery('event_day / 7'),
      'event_month': SQLQuery('event_day / 30'),
      'crystallized': SQLQuery('mmp_crystallized'),
    }

class PerformanceCubeAppsFlyerQuery(PerformanceCubeAppsFlyerBaseQuery):
  @property
  def full_source_name(self) -> str:
    return 'cube_appsflyer'

class PerformanceCubeAppsFlyerKeywordQuery(PerformanceCubeAppsFlyerBaseQuery):
  @property
  def full_source_name(self) -> str:
    return 'cube_appsflyer_keyword'

  @property
  def ad_alias(self) -> Optional[PerformanceCubeAdAlias]:
    return PerformanceCubeAdAlias.keyword

class PerformanceCubeDataLockerQuery(PerformanceCubeAppsFlyerBaseQuery):
  @property
  def full_source_name(self) -> str:
    return 'cube_appsflyer_data_locker'

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'effective_date': SQLQuery('dateadd(day, event_day, daily_cohort)::date'),
      'mmp': LiteralQuery('DataLocker'),
    }

  @property
  def select_event_dates(self) -> bool:
    return False

  @property
  def appsflyer_query(self) -> SQLQuery:
    return CustomSelectQuery(
      source_columns=[
        'product_id',
        'channel',
        'campaign_id',
        'adset_id',
        'ad_id',
        'platform',
        'event_name',
        'daily_cohort',
        'event_day',
        'cohort_events',
        'cohort_revenue',
        'time_series_events',
        'time_series_revenue',
        'mmp_crystallized',
      ],
      source_column_expressions={
        'product_id': SQLQuery('app_id'),
        'daily_cohort': SQLQuery('cohort'),
        'cohort_events': SQLQuery('sum(events)'),
        'cohort_revenue': SQLQuery('sum(revenue)'),
        'time_series_events': SQLQuery('sum(date_events)'),
        'time_series_revenue': SQLQuery('sum(date_revenue)'),
        'mmp_crystallized': SQLQuery('min(crystallized::int)::bool'),
      },
      source_query=CustomUnionQuery(
        select_queries=[
          AppsFlyerDataLockerCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_data_locker',
            cohort=self.task.cohort_anchor,
            ad_alias=self.ad_alias
          ),
          AppsFlyerDataLockerCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_data_locker',
            timeline=PerformanceCubeTimeline.time_series,
            cohort=self.task.cohort_anchor,
            ad_alias=self.ad_alias
          ),
        ]
      ),
      group_by_queries=[
        SQLQuery('app_id'),
        SQLQuery('channel'),
        SQLQuery('campaign_id'),
        SQLQuery('adset_id'),
        SQLQuery('ad_id'),
        SQLQuery('platform'),
        SQLQuery('event_name'),
        SQLQuery('cohort'),
        SQLQuery('event_day'),
      ],
      quote_columns=False
    )

class PerformanceCubeDataLockerKeywordQuery(PerformanceCubeDataLockerQuery):
  @property
  def full_source_name(self) -> str:
    return 'cube_appsflyer_data_locker_keyword'

  @property
  def ad_alias(self) -> Optional[PerformanceCubeAdAlias]:
    return PerformanceCubeAdAlias.keyword

class PerformanceCubeDataLockerOrganicQuery(PerformanceCubeDataLockerQuery):
  @property
  def full_source_name(self) -> str:
    return 'cube_appsflyer_data_locker_organic'

  @property
  def appsflyer_query(self) -> SQLQuery:
    return CustomSelectQuery(
      source_columns=[
        'product_id',
        'platform',
        'event_name',
        'daily_cohort',
        'event_day',
        'cohort_events',
        'cohort_revenue',
        'time_series_events',
        'time_series_revenue',
        'mmp_crystallized',
      ],
      source_column_expressions={
        'daily_cohort': SQLQuery('cohort'),
        'cohort_events': SQLQuery('sum(cohort_events)'),
        'cohort_revenue': SQLQuery('sum(cohort_revenue)'),
        'time_series_events': SQLQuery('sum(time_series_events)'),
        'time_series_revenue': SQLQuery('sum(time_series_revenue)'),
        'mmp_crystallized': SQLQuery('min(mmp_crystallized::int)::bool'),
      },
      source_query=CustomUnionQuery(
        select_queries=[
          AppsFlyerDataLockerOrganicCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_data_locker',
            cohort=PerformanceCubeCohortAnchor.install,
            ad_alias=self.ad_alias
          ),
          AppsFlyerDataLockerOrganicCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_appsflyer_data_locker',
            timeline=PerformanceCubeTimeline.time_series,
            cohort=PerformanceCubeCohortAnchor.install,
            ad_alias=self.ad_alias
          ),
        ]
      ),
      group_by_queries=[
        SQLQuery('product_id'),
        SQLQuery('platform'),
        SQLQuery('event_name'),
        SQLQuery('cohort'),
        SQLQuery('event_day'),
      ],
      quote_columns=False
    )

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'entity_granularity': LiteralQuery(PerformanceCubeEntity.campaign.value),
      'channel': LiteralQuery('organic'),
      'campaign_id': SQLQuery('null'),
      'adset_id': SQLQuery('null'),
      'ad_id': SQLQuery('null'),
    }

class AdjustCubeQuery(SelectQuery):
  schema_prefix: str
  source_table: str
  time_granularity: PerformanceCubeTimeGranularity
  ad_alias: Optional[PerformanceCubeAdAlias]

  def __init__(self, schema_prefix: str, source_table: str, time_granularity: PerformanceCubeTimeGranularity, ad_alias: Optional[PerformanceCubeAdAlias]=None):
    self.schema_prefix = schema_prefix
    self.source_table = source_table
    self.time_granularity = time_granularity
    self.ad_alias = ad_alias
    super().__init__()

  @property
  def full_source_name(self) -> str:
    return f'{self.schema_prefix}{self.source_table}'

  @property
  def ad_id_expression(self) -> SQLQuery:
    if self.ad_alias is None:
      return SQLQuery('creative_id')
    elif self.ad_alias is PerformanceCubeAdAlias.keyword:
      return SQLQuery(f'{self.schema_prefix}keyword_ad_id(campaign_id, adgroup_id, regexp_replace(creative, %s, %s))', (r' \([^)]+\)$', ''))

  @property
  def date_expression(self) -> SQLQuery:
    raise NotImplementedError()

  @property
  def source_columns(self) -> List[str]:
    return [
      'channel',
      'campaign_id',
      'adset_id',
      'ad_id',
      'platform',
      'event_name',
      'country',
      'region',
      'daily_cohort',
      'event_day',
      'event_week',
      'event_month',
      'cohort_events',
      'cohort_revenue',
      'time_series_events',
      'time_series_revenue',
      'converted_users',
      'first_events',
      'mmp_crystallized',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      'channel': SQLQuery(f'{self.schema_prefix}adjust_network_channel(network)'),
      'adset_id': SQLQuery('adgroup_id'),
      'ad_id': self.ad_id_expression,
      'platform': SQLQuery('os_name'),
      'event_day': self.event_time_expression(PerformanceCubeTimeGranularity.day),
      'event_week': self.event_time_expression(PerformanceCubeTimeGranularity.week),
      'event_month': self.event_time_expression(PerformanceCubeTimeGranularity.month),
      'mmp_crystallized': SQLQuery('crystallized'),
    }

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'{self.date_expression.query} >= (select event_start_date from date_range)', self.date_expression.substitution_parameters),
      SQLQuery(f'{self.date_expression.query} < (select event_end_date from date_range)', self.date_expression.substitution_parameters),
    ]

  def event_time_expression(self, time_granularity: PerformanceCubeTimeGranularity) -> SQLQuery:
    raise NotImplementedError()

class AdjustCohortCubeQuery(AdjustCubeQuery):
  @property
  def date_expression(self) -> SQLQuery:
    return SQLQuery(self.time_granularity.value)

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    zero_query = SQLQuery('0')
    return {
      **super().source_column_expressions,
      'daily_cohort': SQLQuery('cohort'),
      'cohort_events': SQLQuery('events'),
      'cohort_revenue': SQLQuery('revenue'),
      'time_series_events': zero_query,
      'time_series_revenue': zero_query,
      'first_events': zero_query,
    }

  def event_time_expression(self, time_granularity: PerformanceCubeTimeGranularity) -> SQLQuery:
    if time_granularity is not self.time_granularity:
      return SQLQuery('null')
    if time_granularity is PerformanceCubeTimeGranularity.day:
      return SQLQuery('datediff(day, cohort, day)')
    elif time_granularity is PerformanceCubeTimeGranularity.week:
      return SQLQuery('datediff(day, cohort, week) / 7')
    elif time_granularity is PerformanceCubeTimeGranularity.month:
      return SQLQuery('datediff(day, cohort, month) / 30')

class AdjustCohortInstallsCubeQuery(AdjustCohortCubeQuery):
  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    zero_query = SQLQuery('0')
    return {
      **super().source_column_expressions,
      'event_name': LiteralQuery('install'),
      'cohort_events': SQLQuery('cohort_size'),
      'cohort_revenue': zero_query,
      'time_series_events': SQLQuery('cohort_size'),
      'converted_users': SQLQuery('cohort_size'),
    }

  def event_time_expression(self, time_granularity: PerformanceCubeTimeGranularity) -> SQLQuery:
    if time_granularity is not self.time_granularity:
      return SQLQuery('null')
    else:
      return SQLQuery('0')

class AdjustTimeSeriesCubeQuery(AdjustCohortCubeQuery):
  @property
  def date_expression(self) -> SQLQuery:
    return SQLQuery("date")

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    zero_query = SQLQuery('0')
    return {
      **super().source_column_expressions,
      'daily_cohort': SQLQuery('"date"'),
      'cohort_events': zero_query,
      'cohort_revenue': zero_query,
      'time_series_events': SQLQuery('events'),
      'time_series_revenue': SQLQuery('revenue'),
      'converted_users': zero_query,
    }

  def event_time_expression(self, time_granularity: PerformanceCubeTimeGranularity) -> SQLQuery:
    if time_granularity is not self.time_granularity:
      return SQLQuery('null')
    else:
      return SQLQuery('0')

class PerformanceCubeMMPBaseQuery(PerformanceCubeSelectQuery):
  @property
  def full_source_name(self) -> str:
    raise NotImplementedError()

  @property
  def ad_alias(self) -> Optional[PerformanceCubeAdAlias]:
    return None

  @property
  def select_event_dates(self) -> bool:
    return True

  @property
  def mmp_query(self) -> SQLQuery:
    raise NotImplementedError()

  @property
  def with_queries(self) -> OrderedDictType[str, any]:
    return OrderedDict([
      ('date_range', CustomSelectQuery(
        source_columns=[
          'start_date',
          'end_date',
          *([
            'event_start_date',
            'event_end_date',
          ] if self.select_event_dates else [])
        ],
        source_column_expressions={
          'event_start_date': SQLQuery('start_date'),
          'event_end_date': SQLQuery('dateadd(day, 2, end_date)'),
        } if self.select_event_dates else {},
        source_query=CustomSelectQuery(
          source_columns=[
            'start_date',
            'end_date',
          ],
          source_column_expressions={
            'start_date': SQLQuery('decode(start_date_fixed, null, decode(start_date_offset, null, null, dateadd(day, start_date_offset, current_date)), start_date_fixed)'),
            'end_date': SQLQuery('decode(end_date_fixed, null, decode(end_date_offset, null, null, dateadd(day, end_date_offset, current_date)), end_date_fixed)')
          },
          full_source_name=f'{self.task.schema_prefix}view_windows',
          condition_queries=[
            SQLQuery('views = %s', ('cube',)),
          ],
          quote_columns=False
        ),
        quote_columns=False
      )),
      (self.full_source_name, self.mmp_query)
    ])

class PerformanceCubeAdjustBaseQuery(PerformanceCubeMMPBaseQuery):
  time_granularity: PerformanceCubeTimeGranularity

  def __init__(self, task: tasks.ReportTask, time_granularity: PerformanceCubeTimeGranularity):
    self.time_granularity = time_granularity
    super().__init__(
      task=task
    )

  @property
  def time_granularity_suffix(self) -> str:
    if self.time_granularity is PerformanceCubeTimeGranularity.day:
      return 'daily'
    else:
      return f'{self.time_granularity.value}ly'

  @property
  def cohort_table(self) -> str:
    return f'fetch_adjust_cohorts_measures_{self.time_granularity_suffix}'

  @property
  def effective_date_expression(self) -> SQLQuery:
    if self.time_granularity is PerformanceCubeTimeGranularity.day:
      return SQLQuery('dateadd(day, event_day, daily_cohort)')
    elif self.time_granularity is PerformanceCubeTimeGranularity.week:
      return SQLQuery('dateadd(day, event_week * 7, daily_cohort)')
    elif self.time_granularity is PerformanceCubeTimeGranularity.month:
      return SQLQuery('dateadd(day, event_month * 30, daily_cohort)')

  @property
  def mmp_query(self) -> SQLQuery:
    return CustomSelectQuery(
      source_columns=[
        'channel',
        'campaign_id',
        'adset_id',
        'ad_id',
        'platform',
        'event_name',
        'daily_cohort',
        'event_day',
        'event_week',
        'event_month',
        'cohort_events',
        'cohort_revenue',
        'time_series_events',
        'time_series_revenue',
        'mmp_crystallized',
        'effective_date',
        'dynamic_segments',
        'dynamic_metrics',
      ],
      source_column_expressions={
        'event_day': self.event_time_expression(PerformanceCubeTimeGranularity.day),
        'event_week': self.event_time_expression(PerformanceCubeTimeGranularity.week),
        'event_month': self.event_time_expression(PerformanceCubeTimeGranularity.month),
        'cohort_events': SQLQuery('sum(cohort_events)'),
        'cohort_revenue': SQLQuery('sum(cohort_revenue)'),
        'time_series_events': SQLQuery('sum(time_series_events)'),
        'time_series_revenue': SQLQuery('sum(time_series_revenue)'),
        'effective_date': self.effective_date_expression,
        'mmp_crystallized': SQLQuery('min(mmp_crystallized::int)::bool'),
        'dynamic_segments': JSONArrayQuery(
          expressions=[
            JSONStringQuery(SQLQuery('country')),
            JSONStringQuery(SQLQuery('region')),
          ],
          item_space=' '
        ),
        'dynamic_metrics': JSONArrayQuery([
          SQLQuery('sum(converted_users)'),
          SQLQuery('sum(first_events)'),
        ]),
      },
      source_query=CustomUnionQuery(
        select_queries=[
          AdjustCohortInstallsCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table=self.cohort_table,
            time_granularity=self.time_granularity,
            ad_alias=self.ad_alias
          ),
          AdjustCohortCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table=self.cohort_table,
            time_granularity=self.time_granularity,
            ad_alias=self.ad_alias
          ),
          AdjustTimeSeriesCubeQuery(
            schema_prefix=self.task.schema_prefix,
            source_table='fetch_adjust_events',
            time_granularity=self.time_granularity,
            ad_alias=self.ad_alias
          ),
        ]
      ),
      group_by_queries=[
        SQLQuery('channel'),
        SQLQuery('campaign_id'),
        SQLQuery('adset_id'),
        SQLQuery('ad_id'),
        SQLQuery('platform'),
        SQLQuery('event_name'),
        SQLQuery('daily_cohort'),
        SQLQuery('event_day'),
        SQLQuery('event_week'),
        SQLQuery('event_month'),
        SQLQuery('dynamic_segments'),
      ],
    )

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'mmp',
      'channel', 
      'campaign_id',
      'adset_id',
      'ad_id',
      'platform',
      'event_name',
      'daily_cohort',
      'weekly_cohort',
      'event_day',
      'event_week',
      'event_month',
      'cohort_events',
      'cohort_revenue',
      'time_series_events',
      'time_series_revenue',
      'mmp_crystallized',
      'crystallized',
      'dynamic_segments',
      'dynamic_metrics',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'row_type': LiteralQuery(PerformanceCubeRowType.aggregation.value),
      'entity_granularity': LiteralQuery(PerformanceCubeEntity.ad.value),
      'cohort_granularity': LiteralQuery(PerformanceCubeCohortGranularity.daily.value),
      'time_granularity': LiteralQuery(self.time_granularity.value),
      'mmp': LiteralQuery('Adjust'),
      'weekly_cohort': SQLQuery(f'{self.task.schema_prefix}monday_of_week(daily_cohort)'),
      'crystallized': SQLQuery('mmp_crystallized'),
    }

  def event_time_expression(self, time_granularity: PerformanceCubeTimeGranularity) -> SQLQuery:
    if self.time_granularity is PerformanceCubeTimeGranularity.day:
      if time_granularity is PerformanceCubeTimeGranularity.day:
        return SQLQuery('event_day')
      elif time_granularity is PerformanceCubeTimeGranularity.week:
        return SQLQuery('event_day / 7')
      elif time_granularity is PerformanceCubeTimeGranularity.month:
        return SQLQuery('event_day / 30')
    elif self.time_granularity is PerformanceCubeTimeGranularity.week:
      if time_granularity is PerformanceCubeTimeGranularity.week:
        return SQLQuery('event_week')
      elif time_granularity is PerformanceCubeTimeGranularity.month:
        return SQLQuery('event_week * 7 / 30')
    elif self.time_granularity is PerformanceCubeTimeGranularity.month:
      if time_granularity is PerformanceCubeTimeGranularity.month:
        return SQLQuery('event_month')
    return SQLQuery('null::int')

class PerformanceCubeAdjustKeywordQuery(PerformanceCubeAdjustBaseQuery):
  @property
  def full_source_name(self) -> str:
    return f'cube_adjust_keyword_{self.time_granularity_suffix}'

  @property
  def ad_alias(self) -> Optional[PerformanceCubeAdAlias]:
    return PerformanceCubeAdAlias.keyword

class PerformanceCubeAdjustQuery(PerformanceCubeSelectQuery):
  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'mmp',
      'channel', 
      'campaign_id',
      'adset_id',
      'ad_id',
      'platform',
      'event_name',
      'daily_cohort',
      'weekly_cohort',
      'event_day',
      'event_week',
      'event_month',
      'cohort_events',
      'cohort_revenue',
      'time_series_events',
      'time_series_revenue',
      'dynamic_segments',
      'dynamic_metrics',
      'mmp_crystallized',
      'crystallized',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'row_type': LiteralQuery(PerformanceCubeRowType.aggregation.value),
      'entity_granularity': LiteralQuery(PerformanceCubeEntity.ad.value),
      'cohort_granularity': LiteralQuery(PerformanceCubeCohortGranularity.daily.value),
      'mmp': LiteralQuery('Adjust'),
      'weekly_cohort': SQLQuery(f'{self.task.schema_prefix}monday_of_week(daily_cohort)'),
      'crystallized': SQLQuery('mmp_crystallized'),
    }

class PerformanceCubeAdjustDailyQuery(PerformanceCubeAdjustQuery):
  @property
  def source_name(self) -> str:
    return 'cube_adjust_daily'

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'time_granularity': LiteralQuery(PerformanceCubeTimeGranularity.day.value),
    }

class PerformanceCubeAdjustWeeklyQuery(PerformanceCubeAdjustQuery):
  @property
  def source_name(self) -> str:
    return 'cube_adjust_weekly'

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'time_granularity': LiteralQuery(PerformanceCubeTimeGranularity.week.value),
    }

class PerformanceCubeAdjustMonthlyQuery(PerformanceCubeAdjustQuery):
  @property
  def source_name(self) -> str:
    return 'cube_adjust_monthly'

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'time_granularity': LiteralQuery(PerformanceCubeTimeGranularity.month.value),
    }

class PerformanceCubeGoogleAdsConversionActionQuery(PerformanceCubeSelectQuery):
  @property
  def source_name(self) -> str:
    return 'cube_google_ads_conversion_action'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'mmp',
      'product_id', 
      'channel', 
      'campaign_id',
      'adset_id',
      'ad_id',
      'platform',
      'event_name',
      'daily_cohort',
      'weekly_cohort',
      'event_day',
      'event_week',
      'event_month',
      'cohort_events',
      'cohort_revenue',
      'time_series_events',
      'time_series_revenue',
      'mmp_crystallized',
      'crystallized',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'effective_date': SQLQuery('dateadd(day, event_day, daily_cohort)::date'),
      'row_type': LiteralQuery(PerformanceCubeRowType.aggregation.value),
      'entity_granularity': LiteralQuery(PerformanceCubeEntity.ad.value),
      'cohort_granularity': LiteralQuery(PerformanceCubeCohortGranularity.daily.value),
      'time_granularity': LiteralQuery(PerformanceCubeTimeGranularity.day.value),
      'mmp': LiteralQuery('Google'),
      'channel': LiteralQuery('Google'),
      'weekly_cohort': SQLQuery(f'{self.task.schema_prefix}monday_of_week(daily_cohort)'),
      'event_week': SQLQuery('event_day / 7'),
      'event_month': SQLQuery('event_day / 30'),
      'crystallized': SQLQuery('mmp_crystallized'),
      **({} if self.task.use_event_value_as_revenue else {'cohort_revenue': SQLQuery('null'), 'time_series_revenue': SQLQuery('null')}),
    }
