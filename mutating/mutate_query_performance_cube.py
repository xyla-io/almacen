import tasks

from enum import Enum
from data_layer import SQLQuery
from models import NullPlaceholder
from .mutate_query_base import GeneratedQuery, SelectQuery, UpdateQuery, DeleteQuery, LiteralQuery, CoalesceComparisonQuery
from typing import List, Dict, OrderedDict as OrderedDictType
from collections import OrderedDict

class PerformanceCubeRowType(Enum):
  aggregation = 'aggregation'
  aggregation_corrected = 'aggregation_corrected'
  granularity_extrapolation = 'granularity_extrapolation'

class PerformanceCubeGranularity(Enum):
  def higher(self, inclusive: bool) -> List[any]:
    granularities = []
    for g in reversed(list(type(self))):
      if not inclusive and g is self:
        break
      granularities.append(g)
      if inclusive and g is self:
        break
    return list(reversed(granularities))
    
  def lower(self, inclusive: bool) -> List[any]:
    granularities = []
    for g in type(self):
      if not inclusive and g is self:
        break
      granularities.append(g)
      if inclusive and g is self:
        break
    return granularities

class PerformanceCubeEntity(PerformanceCubeGranularity):
  ad = 'ad'
  adset = 'adset'
  campaign = 'campaign'

class PerformanceCubeCohortGranularity(PerformanceCubeGranularity):
  daily = 'daily'
  weekly = 'weekly'

class PerformanceCubeTimeGranularity(PerformanceCubeGranularity):
  day = 'day'
  week = 'week'
  month = 'month'

  @property
  def days(self) -> int:
    if self is PerformanceCubeTimeGranularity.day:
      return 1
    elif self is PerformanceCubeTimeGranularity.week:
      return 7
    elif self is PerformanceCubeTimeGranularity.month:
      return 30

class PerformanceCubeUpdateQuery(UpdateQuery):
  task: tasks.ReportTask
  
  def __init__(self, task: tasks.ReportTask, full_target_name: str):
    self.task = task
    super().__init__(full_target_name=full_target_name)

class PerformanceCubeDeleteQuery(DeleteQuery):
  task: tasks.ReportTask
  
  def __init__(self, task: tasks.ReportTask, full_target_name: str):
    self.task = task
    super().__init__(full_target_name=full_target_name)

class PerformanceCubeSelectQuery(SelectQuery):
  task: tasks.ReportTask

  def __init__(self, task: tasks.ReportTask):
    self.task = task
    super().__init__()

  @property
  def source_name(self) -> str:
    raise NotImplementedError()

  @property
  def full_source_name(self) -> str:
    return f'{self.task.schema_prefix}{self.source_name}' if self.source_name else ''

  @property
  def source_columns(self) -> List[str]:
    return [
      'source',
      'row_type',
      'effective_date',
      'entity_granularity',
      'cohort_granularity',
      'time_granularity',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      'source': LiteralQuery(self.task.task_type.value),
    }

class PerformanceCubeChannelSelectQuery(PerformanceCubeSelectQuery):
  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      *[f'{g.value}_id' for g in self.entity_granularity.higher(inclusive=True)],
      *[f'{g.value}_name' for g in self.entity_granularity.higher(inclusive=True)],
      'product_name',
      'platform',
      'channel',
      'channel_crystallized',
      'crystallized',
      'daily_cohort',
      'weekly_cohort',
      'event_day',
      'event_week',
      'event_month',
      'spend',
      'impressions',
      'clicks',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'row_type': LiteralQuery(PerformanceCubeRowType.aggregation.value),
      'entity_granularity': LiteralQuery(self.entity_granularity.value),
      'cohort_granularity': LiteralQuery(PerformanceCubeCohortGranularity.daily.value),
      'time_granularity': LiteralQuery(PerformanceCubeTimeGranularity.day.value),
      'channel': LiteralQuery(self.channel_name),
      'channel_crystallized': SQLQuery('crystallized'),
      'daily_cohort': SQLQuery('effective_date'),
      'weekly_cohort': SQLQuery(f'{self.task.schema_prefix}monday_of_week(effective_date)'),
      'event_day': LiteralQuery(0),
      'event_week': LiteralQuery(0),
      'event_month': LiteralQuery(0),
    }
  
  @property
  def channel_name(self) -> str:
    pass
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    pass

class PerformanceCubeGranulartiySelectQuery(SelectQuery):
  full_target_name: str
  entity_granularity: PerformanceCubeEntity
  cohort_granularity: PerformanceCubeCohortGranularity
  time_granularity: PerformanceCubeTimeGranularity
  lower_entities: List[PerformanceCubeEntity]
  lower_cohorts: List[PerformanceCubeCohortGranularity]
  lower_times: List[PerformanceCubeTimeGranularity]

  def __init__(self, task: tasks.ReportTask,  full_target_name: str, entity_granularity: PerformanceCubeEntity, cohort_granularity: PerformanceCubeCohortGranularity, time_granularity: PerformanceCubeTimeGranularity):
    self.task = task
    self.full_target_name = full_target_name
    self.entity_granularity = entity_granularity
    self.cohort_granularity = cohort_granularity
    self.time_granularity = time_granularity
    self.lower_entities = [g.value for g in self.entity_granularity.lower(False)]
    self.lower_cohorts = [g.value for g in self.cohort_granularity.lower(False)]
    self.lower_times = [g.value for g in self.time_granularity.lower(False)]
    super().__init__()

  @property
  def full_source_name(self) -> str:
    return self.full_target_name

  @property
  def source_columns(self) -> List[str]:
    return [
      'effective_date',
      'source',
      'row_type',
      'entity_granularity',
      'cohort_granularity',
      'time_granularity',
    ]

  @property
  def condition_queries(self) -> List[SQLQuery]:
    condition_queries = []
    if self.lower_entities:
      condition_queries.append(SQLQuery(f'entity_granularity in {SQLQuery.format_array(self.lower_entities)}', tuple(self.lower_entities)))
    if self.lower_cohorts:
      condition_queries.append(SQLQuery(f'cohort_granularity in {SQLQuery.format_array(self.lower_cohorts)}', tuple(self.lower_cohorts)))
    if self.lower_times:
      condition_queries.append(SQLQuery(f'time_granularity in {SQLQuery.format_array(self.lower_times)}', tuple(self.lower_times)))
    if not condition_queries:
      return [SQLQuery('FALSE')]
    inclusive_lower_entities = [g.value for g in self.entity_granularity.lower(True)]
    inclusive_lower_cohorts = [g.value for g in self.cohort_granularity.lower(True)]
    inclusive_lower_times = [g.value for g in self.time_granularity.lower(True)]
    return [
      SQLQuery(f'entity_granularity in {SQLQuery.format_array(inclusive_lower_entities)}', tuple(inclusive_lower_entities)),
      SQLQuery(f'cohort_granularity in {SQLQuery.format_array(inclusive_lower_cohorts)}', tuple(inclusive_lower_cohorts)),
      SQLQuery(f'time_granularity in {SQLQuery.format_array(inclusive_lower_times)}', tuple(inclusive_lower_times)),
      SQLQuery(
        query='(' + '\nor '.join([c.query for c in condition_queries]) + ')',
        substitution_parameters=tuple(p for c in condition_queries for p in c.substitution_parameters)
      ),
    ]

class PerformanceCubeGranularityExtrapolationQuery(PerformanceCubeGranulartiySelectQuery):
  dynamic_metric_count: int
  target_platform: bool

  def __init__(self, task: tasks.ReportTask,  full_target_name: str, entity_granularity: PerformanceCubeEntity, cohort_granularity: PerformanceCubeCohortGranularity, time_granularity: PerformanceCubeTimeGranularity, dynamic_metric_count: int=0, target_platform: bool=True):
    self.dynamic_metric_count = dynamic_metric_count
    self.target_platform = target_platform
    super().__init__(task=task, full_target_name=full_target_name, entity_granularity=entity_granularity, cohort_granularity=cohort_granularity, time_granularity=time_granularity)

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      *self.target_columns,
      'spend',
      'impressions',
      'clicks',
      'reach',
      'cohort_events',
      'cohort_revenue',
      'time_series_events',
      'time_series_revenue',
      'channel_crystallized',
      'mmp_crystallized',
      'dynamic_metrics',
    ]

  @property
  def metric_aggregators(self) -> OrderedDictType[str, str]:
    dynamic_metric_extraction_text = "\n|| ',' || ".join(
      f"sum(coalesce(json_extract_array_element_text(decode(dynamic_metrics, '', null, dynamic_metrics), {m}, true)::double precision, 0))"
      for m in range(0, self.dynamic_metric_count)
    )
    return OrderedDict([
      ('spend', 'sum(spend)'),
      ('impressions', 'sum(impressions)'),
      ('clicks', 'sum(clicks)'),
      ('reach', 'sum(reach)'),
      ('cohort_events', 'sum(cohort_events)'),
      ('cohort_revenue', 'sum(cohort_revenue)'),
      ('time_series_events', 'sum(time_series_events)'),
      ('time_series_revenue', 'sum(time_series_revenue)'),
      ('channel_crystallized', 'min(channel_crystallized::int)::bool'),
      ('mmp_crystallized', 'min(mmp_crystallized::int)::bool'),
      ('dynamic_metrics', f"'[' || {dynamic_metric_extraction_text} || ']'" if self.dynamic_metric_count else 'null'),
    ])

  @property
  def all_metrics_query(self) -> SQLQuery:
    return SQLQuery(', '.join([f'{v} as "{k}"' for k, v in self.metric_aggregators.items()]))

  @property
  def target_columns(self) -> List[str]:
    return [
      'channel',
      *(['platform'] if self.target_platform else []),
      'mmp',
      'event_name',
      *[f'{e.value}_id' for e in PerformanceCubeEntity if e.value not in self.lower_entities],
      *[f'{c.value}_cohort' for c in PerformanceCubeCohortGranularity if c.value not in self.lower_cohorts],
      *[f'event_{t.value}' for t in PerformanceCubeTimeGranularity if t.value not in self.lower_times],
      'dynamic_segments',
    ]

  @property
  def target_column_coalesce_values(self) -> Dict[str, str]:
    return {
      'channel': NullPlaceholder.string.value,
      'platform': NullPlaceholder.string.value,
      'mmp': NullPlaceholder.string.value,
      'event_name': NullPlaceholder.string.value,
      **{f'{e.value}_id': NullPlaceholder.string.value for e in PerformanceCubeEntity if e.value not in self.lower_entities},
      **{f'{c.value}_cohort': NullPlaceholder.date_string.value for c in PerformanceCubeCohortGranularity if c.value not in self.lower_cohorts},
      **{f'event_{t.value}': NullPlaceholder.integer.value for t in PerformanceCubeTimeGranularity if t.value not in self.lower_times},
      'dynamic_segments': NullPlaceholder.string.value,
    }

  def generate_query(self):
    select_targets = ', '.join(self.target_columns)
    self.query = f'''
select 
dateadd(day, event_{self.time_granularity.value} * {self.time_granularity.days}, {self.cohort_granularity.value}_cohort)::date as effective_date,
%s as source, %s as row_type, %s as entity_granularity, %s as cohort_granularity, %s as time_granularity, 
{select_targets},
{self.all_metrics_query.query}
from
(
  select {select_targets},
  spend, impressions, clicks, reach, cohort_events, cohort_revenue, time_series_events, time_series_revenue,
  channel_crystallized, mmp_crystallized, dynamic_metrics
  from {self.full_target_name}
  {self.all_conditions_query.query}
)
group by {select_targets}
    '''
    self.substitution_parameters = (
      self.task.task_type.value, 
      PerformanceCubeRowType.granularity_extrapolation.value, 
      self.entity_granularity.value,
      self.cohort_granularity.value,
      self.time_granularity.value,
      *self.all_metrics_query.substitution_parameters,
      *self.all_conditions_query.substitution_parameters,
    )

class PerformanceCubeGranularityUpdateQuery(PerformanceCubeUpdateQuery):
  entity_granularity: PerformanceCubeEntity
  cohort_granularity: PerformanceCubeCohortGranularity
  time_granularity: PerformanceCubeTimeGranularity
  extrapolation_query: PerformanceCubeGranularityExtrapolationQuery

  def __init__(self, task: tasks.ReportTask,  full_target_name: str, entity_granularity: PerformanceCubeEntity, cohort_granularity: PerformanceCubeCohortGranularity, time_granularity: PerformanceCubeTimeGranularity, dynamic_metric_count: int=0, target_platform: bool=True):
    self.entity_granularity = entity_granularity
    self.cohort_granularity = cohort_granularity
    self.time_granularity = time_granularity
    self.extrapolation_query = PerformanceCubeGranularityExtrapolationQuery(
      task=task,
      full_target_name=full_target_name,
      entity_granularity=self.entity_granularity,
      cohort_granularity=self.cohort_granularity,
      time_granularity=self.time_granularity,
      dynamic_metric_count=dynamic_metric_count,
      target_platform=target_platform
    )
    super().__init__(task=task, full_target_name=full_target_name)

  @property
  def set_queries(self) -> List[SQLQuery]:
    dynamic_metric_correction_text = "\n|| ',' || ".join(
      f"coalesce(json_extract_array_element_text(decode({self.full_target_name}.dynamic_metrics, '', null, {self.full_target_name}.dynamic_metrics), {m}, true)::double precision, 0) - coalesce(json_extract_array_element_text(decode(e.dynamic_metrics, '', null, e.dynamic_metrics), {m}, true)::double precision, 0)"
      for m in range(0, self.extrapolation_query.dynamic_metric_count)
    )
    return [
      SQLQuery(f'row_type = %s', (PerformanceCubeRowType.aggregation_corrected.value,)),
      SQLQuery(f'spend = {self.full_target_name}.spend - e.spend'),
      SQLQuery(f'impressions = {self.full_target_name}.impressions - e.impressions'),
      SQLQuery(f'clicks = {self.full_target_name}.clicks - e.clicks'),
      SQLQuery(f'reach = {self.full_target_name}.reach - e.reach'),
      SQLQuery(f'cohort_events = {self.full_target_name}.cohort_events - e.cohort_events'),
      SQLQuery(f'cohort_revenue = {self.full_target_name}.cohort_revenue - e.cohort_revenue'),
      SQLQuery(f'time_series_events = {self.full_target_name}.time_series_events - e.time_series_events'),
      SQLQuery(f'time_series_revenue = {self.full_target_name}.time_series_revenue - e.time_series_revenue'),
      SQLQuery(f"dynamic_metrics = '[' || {dynamic_metric_correction_text} || ']'" if self.extrapolation_query.dynamic_metric_count else 'dynamic_metrics = null')
    ]
  
  @property
  def from_queries(self) -> OrderedDictType[str, SQLQuery]:
    return OrderedDict([
      ('e', SQLQuery(f'({self.extrapolation_query.query})', self.extrapolation_query.substitution_parameters))
    ])

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'{self.full_target_name}.entity_granularity = %s', (self.entity_granularity.value,)),
      SQLQuery(f'{self.full_target_name}.cohort_granularity = %s', (self.cohort_granularity.value,)),
      SQLQuery(f'{self.full_target_name}.time_granularity = %s',  (self.time_granularity.value,)),
      *[
        CoalesceComparisonQuery(
          leftQuery=SQLQuery(f'{self.full_target_name}."{t}"'),
          rightQuery=SQLQuery(f'e."{t}"'),
          nullPlaceholder=self.extrapolation_query.target_column_coalesce_values[t]
        ) if t in self.extrapolation_query.target_column_coalesce_values else SQLQuery(f'{self.full_target_name}."{t}" = e."{t}"')
        for t in self.extrapolation_query.target_columns
      ],
    ]

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

class PerformanceCubeGlobalTagResetQuery(PerformanceCubeUpdateQuery):
  @property
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('ad_tag = null'),
      SQLQuery('ad_subtag = null'),
      SQLQuery('adset_tag = null'),
      SQLQuery('adset_subtag = null'),
      SQLQuery('campaign_tag = null'),
      SQLQuery('campaign_subtag = null'),
    ]

class PerformanceCubeGlobalTagUpdateQuery(PerformanceCubeUpdateQuery):
  entity: PerformanceCubeEntity

  def __init__(self, task: tasks.ReportTask, full_target_name: str, entity: PerformanceCubeEntity):
    self.entity = entity
    super().__init__(task=task, full_target_name=full_target_name)

  @property
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'{self.entity.value}_tag = t.{self.entity.value}_tag'),
      SQLQuery(f'{self.entity.value}_subtag = t.{self.entity.value}_subtag'),
    ]

  @property
  def from_queries(self) -> OrderedDictType[str, SQLQuery]:
    return OrderedDict([
      ('t', SQLQuery(f'{self.task.schema_prefix}tag_{self.entity.value}s')),
    ])

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'{self.full_target_name}.channel = t.channel'),
      SQLQuery(f'{self.full_target_name}.{self.entity.value}_id = t.{self.entity.value}_id'),
    ]