import tasks

from typing import List, Dict
from data_layer import SQLQuery
from .mutate_query_base import LatestUpdateQuery, InsertQuery, LatestSelectQuery, SelectQuery, LiteralQuery
from .mutate_query_performance_cube import PerformanceCubeEntity
from .mutate_performance_cube import StagedQueryReplaceMutator

class EntitySelectQuery(SelectQuery):
  task: tasks.ReportTask

  def __init__(self, task: tasks.ReportTask):
    self.task = task
    super().__init__()

  @property
  def full_source_name(self) -> str:
    return f'{self.task.schema_prefix}performance_cube_filtered'
  
  @property
  def entity(self) -> PerformanceCubeEntity:
    pass

  @property
  def source_columns(self) -> List[str]:
    return [
      'channel',
      *[f'{e.value}_id' for e in self.entity.higher(inclusive=True)],
      f'{self.entity.value}_name',
      'last_active_date',
      'fetch_date',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      f'{self.entity.value}_name': SQLQuery(f'max({self.entity.value}_name)'),
      'last_active_date': SQLQuery('max(daily_cohort)'),
      'fetch_date': LiteralQuery(SQLQuery.format_date(self.task.run_date)),
    }

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'{self.entity.value}_id is not null'),
      SQLQuery(f'{self.entity.value}_id != %s', ('',)),
      *([] if self.task.materialize_derived_entities else [SQLQuery(f'{self.entity.value}_id not like %s', ('flx-%',)), SQLQuery(f'{self.entity.value}_id != %s', ('-1',))]),
    ]

  @property
  def group_by_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('channel'),
      *[SQLQuery(f'{e.value}_id') for e in self.entity.higher(inclusive=True)],
    ]

class EntityMutator(StagedQueryReplaceMutator[tasks.ReportTask]):
  @property
  def entity_query(self) -> EntitySelectQuery:
    pass

  @property
  def target_columns(self) -> List[str]:
    return [
      'channel',
      *[f'{e.value}_id' for e in self.entity_query.entity.higher(inclusive=True)]
    ]

  @property
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=self.entity_query
      ),
      LatestUpdateQuery(
        full_target_name=self.stage_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}performance_cube_filtered',
          date_column='daily_cohort',
          value_column=f'{self.entity_query.entity.value}_status',
          target_columns=self.target_columns,
          condition_queries=[SQLQuery('mmp is null')]
        ),
      ),
      LatestUpdateQuery(
        full_target_name=self.stage_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}performance_cube_filtered',
          date_column='daily_cohort',
          value_column='product_id',
          target_columns=self.target_columns,
          condition_queries=[SQLQuery('mmp is null')]
        ),
      ),
      LatestUpdateQuery(
        full_target_name=self.stage_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}performance_cube_filtered',
          date_column='daily_cohort',
          value_column='product_name',
          target_columns=self.target_columns,
          condition_queries=[SQLQuery('mmp is null')]
        ),
      ),
      LatestUpdateQuery(
        full_target_name=self.stage_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}performance_cube_filtered',
          date_column='daily_cohort',
          value_column='platform',
          target_columns=self.target_columns,
          condition_queries=[SQLQuery('mmp is null')]
        ),
        source_to_target_columns={'platform': 'product_platform'},
      ),
    ]

  @property
  def insert_query(self) -> SQLQuery:
    columns = '(' + ', '.join([f'"{c}"' for c in self.source_columns]) + ') ' if self.source_columns else ''
    return SQLQuery(f'INSERT INTO {self.stage_table_name} {columns} ({self.source_query.query});', substitution_parameters=self.source_query.substitution_parameters)


# -----------------------------------------------------------------------------
# Campaign
# -----------------------------------------------------------------------------
class EntityCampaignQuery(EntitySelectQuery):
  @property
  def entity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.campaign

class EntityCampaignMutator(EntityMutator):
  @property
  def entity_query(self) -> EntitySelectQuery:
    return EntityCampaignQuery(task=self.task)

# -----------------------------------------------------------------------------
# Adset
# -----------------------------------------------------------------------------
class EntityAdsetQuery(EntityCampaignQuery):
  @property
  def entity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.adset

class EntityAdsetMutator(EntityCampaignMutator):
  @property
  def entity_query(self) -> EntitySelectQuery:
    return EntityAdsetQuery(task=self.task)

# -----------------------------------------------------------------------------
# Ad
# -----------------------------------------------------------------------------
class EntityAdQuery(EntityCampaignQuery):
  @property
  def entity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.ad

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'ad_type',
      'creative_url',
    ]

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'ad_type': SQLQuery('max(ad_type)'),
      'creative_url': SQLQuery(
        query='max(coalesce(decode(creative_image_url, %s, null, creative_image_url), creative_thumbnail_url))',
        substitution_parameters=('',)
      ),
    }

class EntityAdMutator(EntityCampaignMutator):
  @property
  def entity_query(self) -> EntitySelectQuery:
    return EntityAdQuery(task=self.task)

  @property
  def stage_queries(self) -> List[SQLQuery]:
    return [
      *super().stage_queries,
      LatestUpdateQuery(
        full_target_name=self.stage_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}performance_cube_filtered',
          date_column='daily_cohort',
          value_column='creative_url',
          target_columns=self.target_columns,
          value_query=SQLQuery(
            query='coalesce(decode(creative_image_url, %s, null, creative_image_url), creative_thumbnail_url)',
            substitution_parameters=('',)
          ),
          condition_queries=[
            SQLQuery('mmp is null'),
            SQLQuery(
              query='coalesce(decode(creative_image_url, %s, null, creative_image_url), decode(creative_thumbnail_url, %s, null, creative_thumbnail_url)) is not null',
              substitution_parameters=('', '')
            ),
          ]
        ),
      ),
    ]