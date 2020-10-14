from data_layer import SQLQuery
from models import NullPlaceholder
from .mutate_query_base import DeleteQuery, CoalesceComparisonQuery
from .mutate_query_performance_cube import PerformanceCubeRowType, PerformanceCubeEntity, PerformanceCubeSelectQuery, PerformanceCubeUpdateQuery, PerformanceCubeDeleteQuery
from typing import List, Dict, OrderedDict as OrderedDictType
from collections import OrderedDict
from datetime import timedelta

# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------

class PerformanceCubeFilterBaseSelectQuery(PerformanceCubeSelectQuery):
  @property
  def source_name(self) -> str:
    return 'performance_cube_unfiltered'

  @property
  def source_columns(self) -> List[str]:
    return []

  @property
  def target_row_types(self) -> List[str]:
    return [
      PerformanceCubeRowType.aggregation.value, 
    ]

  @property
  def date_condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'effective_date >= %s', (SQLQuery.format_date(self.task.report_start_date),)),
      SQLQuery(f'effective_date <= %s', (SQLQuery.format_date(self.task.report_end_date),)),
    ]

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'row_type in {SQLQuery.format_array(self.target_row_types)}', tuple(self.target_row_types)),
      *self.date_condition_queries,
    ]

# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------

class FilterPerformanceCubeChannelsSelectQuery(PerformanceCubeFilterBaseSelectQuery):
  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      *super().condition_queries,
      SQLQuery('mmp is null'),
      SQLQuery('(coalesce(spend, 0) != 0 or coalesce(impressions, 0) != 0 or coalesce(clicks, 0) != 0 or coalesce(reach, 0) != 0)'),
    ]

class FilterPerformanceCubeChannelsUpdateQuery(PerformanceCubeUpdateQuery):
  @property
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('source = %s', (self.task.task_type.value,))
    ]

# ---------------------------------------------------------------------------
# MMPs
# ---------------------------------------------------------------------------

class FilterPerformanceCubeMMPsBaseSelectQuery(PerformanceCubeFilterBaseSelectQuery):
  @property
  def target_row_types(self) -> List[str]:
    return [
      PerformanceCubeRowType.aggregation.value, 
      PerformanceCubeRowType.granularity_extrapolation.value, 
    ]

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      *super().condition_queries,
      SQLQuery('mmp is not null'),
      SQLQuery('(coalesce(cohort_events, 0) != 0 or coalesce(cohort_revenue, 0) != 0 or coalesce(time_series_events, 0) != 0 or coalesce(time_series_revenue, 0) != 0)'),
      SQLQuery('not (coalesce(event_day, 0) < 0 or coalesce(event_week, 0) < 0 or coalesce(event_month, 0) < 0)'),
    ]

class FilterPerformanceCubeMMPsSelectQuery(FilterPerformanceCubeMMPsBaseSelectQuery):
  @property
  def date_condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'effective_date >= %s', (SQLQuery.format_date(self.task.report_start_date),)),
      # Add 35 days because a week starting on the last day of the last month in the report (report_end_date + 29 days) will end 6 days later (report_end_date + 35 days)
      SQLQuery(f'effective_date <= %s', (SQLQuery.format_date(self.task.report_end_date + timedelta(days=35)),)),
    ]

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      *super().condition_queries,
      SQLQuery('channel != %s', ('organic',))
    ]

class FilterPerformanceCubeMMPsMatchUpdateQuery(PerformanceCubeUpdateQuery):
  @property
  def set_queries(self) -> List[SQLQuery]:
    queries = [
      SQLQuery('source = %s', (self.task.task_type.value,)),
      SQLQuery('product_name = c.product_name'),
    ]
    if self.task.infer_channel:
      queries.append(SQLQuery('channel = c.channel'))
    return queries

  @property
  def from_queries(self) -> OrderedDictType[str, SQLQuery]:
    entity_id_columns = ', '.join(f'{g.value}_id' for g in PerformanceCubeEntity)
    return OrderedDict([
      ('c', SQLQuery(f'''
(select channel, {entity_id_columns}, max(product_name) as product_name
from {self.task.report_table_model.full_table_name}
where mmp is null
group by channel, {entity_id_columns})
      ''')),
    ])

  @property
  def condition_queries(self) -> List[SQLQuery]:
    queries = [
      *[
        CoalesceComparisonQuery(
          leftQuery=SQLQuery(f'{self.full_target_name}.{g.value}_id'), 
          rightQuery=SQLQuery(f'c.{g.value}_id'), 
          nullPlaceholder=NullPlaceholder.string.value
        )
        for g in PerformanceCubeEntity
      ],
    ]
    if not self.task.infer_channel:
      queries.append(SQLQuery(f'{self.full_target_name}.channel = c.channel'),)
    return queries

class FilterPerformanceCubeMMPsUpdateQuery(PerformanceCubeUpdateQuery):
  @property
  def set_queries(self) -> List[SQLQuery]:
    queries = [
      SQLQuery('source = %s', (self.task.task_type.value,)),
      SQLQuery('product_name = c.product_name'),
      SQLQuery('campaign_name = c.campaign_name'),
      SQLQuery('adset_name = c.adset_name'),
      SQLQuery('ad_name = c.ad_name'),
      SQLQuery('campaign_tag = c.campaign_tag'),
      SQLQuery('campaign_subtag = c.campaign_subtag'),
      SQLQuery('adset_tag = c.adset_tag'),
      SQLQuery('adset_subtag = c.adset_subtag'),
      SQLQuery('ad_tag = c.ad_tag'),
      SQLQuery('ad_subtag = c.ad_subtag'),
      SQLQuery('ad_type = c.ad_type'),
      SQLQuery('campaign_status = c.campaign_status'),
      SQLQuery('adset_status = c.adset_status'),
      SQLQuery('ad_status = c.ad_status'),
      SQLQuery('first_impression_date = c.first_impression_date'),
      SQLQuery('campaign_objective = c.campaign_objective'),
      SQLQuery('creative_id = c.creative_id'),
      SQLQuery('creative_name = c.creative_name'),
      SQLQuery('creative_title = c.creative_title'),
      SQLQuery('creative_body = c.creative_body'),
      SQLQuery('creative_image_url = c.creative_image_url'),
      SQLQuery('creative_thumbnail_url = c.creative_thumbnail_url'),
      SQLQuery('channel_crystallized = c.channel_crystallized'),
    ]
    if self.task.infer_channel:
      queries.append(SQLQuery('channel = c.channel'))
    return queries

  @property
  def from_queries(self) -> OrderedDictType[str, SQLQuery]:
    return OrderedDict([
      ('c', SQLQuery(self.task.report_table_model.full_table_name)),
    ])

  @property
  def condition_queries(self) -> List[SQLQuery]:
    queries = [
      SQLQuery(f'c.event_day = %s', (0,)),
      SQLQuery(f'c.mmp is null'),
      SQLQuery(f'{self.full_target_name}.daily_cohort = c.daily_cohort'),
      *[
        CoalesceComparisonQuery(
          leftQuery=SQLQuery(f'{self.full_target_name}.{g.value}_id'), 
          rightQuery=SQLQuery(f'c.{g.value}_id'), 
          nullPlaceholder=NullPlaceholder.string.value
        )
        for g in PerformanceCubeEntity
      ],
    ]
    if not self.task.infer_channel:
      queries.append(SQLQuery(f'{self.full_target_name}.channel = c.channel'),)
    return queries

class FilterPerformanceCubeMMPsUnmatchedDeleteQuery(PerformanceCubeDeleteQuery):
  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('source != %s', (self.task.task_type.value,)),
    ]

class FilterPerformanceCubeMMPsDateDeleteQuery(PerformanceCubeDeleteQuery):
  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('effective_date > %s', (self.task.report_end_date,)),
    ]

class FilterPerformanceCubeMMPsCrystallizationQuery(PerformanceCubeUpdateQuery):
  @property
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('crystallized = (channel_crystallized or channel_crystallized is null) and mmp_crystallized'),
    ]

class FilterPerformanceCubeOrganicSelectQuery(FilterPerformanceCubeMMPsBaseSelectQuery):
  @property
  def target_row_types(self) -> List[str]:
    return [
      PerformanceCubeRowType.aggregation.value, 
    ]

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      *super().condition_queries,
      SQLQuery('channel = %s', ('organic',)),
    ]

class FilterPerformanceCubeOrganicUpdateQuery(PerformanceCubeUpdateQuery):
  @property
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('source = %s', (self.task.task_type.value,))
    ]
