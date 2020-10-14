import tasks

from . import base
from data_layer import SQLQuery
from .mutate_query_base import InsertQuery, DeleteQuery, LatestSelectQuery, LatestUpdateQuery
from .mutate_query_performance_cube import PerformanceCubeEntity, PerformanceCubeCohortGranularity, PerformanceCubeTimeGranularity, PerformanceCubeGranularityExtrapolationQuery, PerformanceCubeGranularityUpdateQuery, PerformanceCubeGlobalTagResetQuery, PerformanceCubeGlobalTagUpdateQuery
from .mutate_query_performance_cube_channel import AppleKeywordPerformanceCubeQuery, AppleCreativeSetPerformanceCubeQuery, AppleAdGroupPerformanceCubeQuery, AppleCampaignPerformanceCubeQuery, FacebookCampaignPerformanceCubeQuery, FacebookAdSetPerformanceCubeQuery, FacebookAdPerformanceCubeQuery, SnapchatAdPerformanceCubeQuery, SnapchatAdSquadPerformanceCubeQuery, SnapchatCampaignPerformanceCubeQuery, TikTokAdPerformanceCubeQuery, TikTokAdGroupPerformanceCubeQuery, TikTokCampaignPerformanceCubeQuery, GoogleAdsAdPerformanceCubeQuery, GoogleAdsCampaignPerformanceCubeQuery, GoogleAdsAdGroupPerformanceCubeQuery
from .mutate_query_performance_cube_mmp import PerformanceCubeMMPDeleteQuery, PerformanceCubeAppsFlyerQuery, PerformanceCubeAppsFlyerKeywordQuery, PerformanceCubeDataLockerQuery, PerformanceCubeDataLockerKeywordQuery, PerformanceCubeDataLockerOrganicQuery, PerformanceCubeAdjustDailyQuery, PerformanceCubeAdjustWeeklyQuery, PerformanceCubeAdjustMonthlyQuery, PerformanceCubeAdjustKeywordQuery, PerformanceCubeGoogleAdsConversionActionQuery
from .mutate_query_performance_cube_filter import FilterPerformanceCubeChannelsSelectQuery, FilterPerformanceCubeChannelsUpdateQuery, FilterPerformanceCubeMMPsSelectQuery, FilterPerformanceCubeMMPsMatchUpdateQuery, FilterPerformanceCubeMMPsUpdateQuery, FilterPerformanceCubeMMPsUnmatchedDeleteQuery, FilterPerformanceCubeMMPsDateDeleteQuery, FilterPerformanceCubeMMPsCrystallizationQuery, FilterPerformanceCubeOrganicSelectQuery, FilterPerformanceCubeOrganicUpdateQuery
from typing import TypeVar, Generic, Optional, List, Dict, OrderedDict as OrderedDictType
from collections import OrderedDict
from abc import abstractmethod
from enum import Enum

T = TypeVar(tasks.MutateReportTask)
class StagedQueryBaseMutator(Generic[T], base.QueryReportMutator[T]):
  @property
  def stage_queries(self) -> List[SQLQuery]:
    return []

  @property
  def all_stages_query(self) -> SQLQuery:
    return SQLQuery(
      query='\n'.join([f'{q.query};' for q in self.stage_queries]),
      substitution_parameters=tuple(p for q in self.stage_queries for p in q.substitution_parameters)
    )

  @property
  def query(self) -> SQLQuery:
    return SQLQuery(
      query=f'''
begin transaction;
{self.all_stages_query.query}
end transaction;
      ''',
      substitution_parameters=self.all_stages_query.substitution_parameters
    )

  def mutate(self):
    print(f'{self.task.task_type.value} staged query\n\n{self.query.substituted_query}')
    return super().mutate()

class StagedQueryReplaceMutator(Generic[T], StagedQueryBaseMutator[T]):
  @property
  def is_row_count_query(self) -> bool:
    return True

  @property
  def stage_table_name(self) -> str:
    return f'stage_{self.task.report_table_model.table_name}'

  @property
  def query(self) -> SQLQuery:
    staged_query = super().query
    return SQLQuery(
      query=f'''
create temporary table {self.stage_table_name} (like {self.task.report_table_model.full_table_name} including defaults);
{staged_query.query}
begin transaction;
delete from {self.task.report_table_model.full_table_name};
insert into {self.task.report_table_model.full_table_name} (select * from {self.stage_table_name});
end transaction;
drop table {self.stage_table_name};
select count(*) from {self.task.report_table_model.full_table_name};
      ''',
      substitution_parameters=staged_query.substitution_parameters
    )

U = TypeVar(tasks.MaterializePerformanceCubeUnfilteredBaseTask)
class MaterializePerformanceCubeBaseMutator(Generic[U], base.MaterializeMutator[U], StagedQueryBaseMutator[U]):
  @property 
  def insert_query(self) -> SQLQuery:
    return self.all_stages_query

  @property
  def delete_query(self) -> SQLQuery:
    identifier_conditions_query = SQLQuery('')
    self.task.append_identifier_column_conditions_to_query(identifier_conditions_query)
    return SQLQuery(f'''
delete from {self.task.report_table_model.full_table_name}
where effective_date >= %s
and effective_date <= %s
and {identifier_conditions_query.query};
      ''',
      substitution_parameters=tuple([
        SQLQuery.format_date(self.task.report_start_date),
        SQLQuery.format_date(self.task.report_end_date),
        *identifier_conditions_query.substitution_parameters,
      ])
    )

class PerformanceCubeUnfilteredBaseMutator(Generic[U], MaterializePerformanceCubeBaseMutator[U]):
  @property
  def view_windows_views(self) -> str:
    return 'cube'

  @property
  def prepare_query(self) -> SQLQuery:
    return SQLQuery(
      query=f'''
update {self.task.schema_prefix}view_windows
set start_date_fixed = %s,
end_date_fixed = %s
where views = %s;
      ''',
      substitution_parameters=(
        SQLQuery.format_date(self.task.report_start_date),
        SQLQuery.format_date(self.task.report_end_date),
        self.view_windows_views
      )
    )

  @property
  def cleanup_query(self) -> SQLQuery:
    return SQLQuery(
      query=f'''
update {self.task.schema_prefix}view_windows
set start_date_fixed = null,
end_date_fixed = null
where views = %s;
      ''',
      substitution_parameters=(self.view_windows_views,)
    )

class ApplePerformanceCubeMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeAppleTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=AppleKeywordPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=AppleCreativeSetPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=AppleAdGroupPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=AppleCampaignPerformanceCubeQuery(task=self.task)
      ),
    ]

class FacebookPerformanceCubeMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeFacebookTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=FacebookAdPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=FacebookAdSetPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=FacebookCampaignPerformanceCubeQuery(task=self.task)
      ),
    ]

class GoogleAdsPerformanceCubeMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeGoogleAdsTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=GoogleAdsAdPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=GoogleAdsAdGroupPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=GoogleAdsCampaignPerformanceCubeQuery(task=self.task)
      ),
    ]

class SnapchatPerformanceCubeMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeSnapchatTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=SnapchatAdPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=SnapchatAdSquadPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=SnapchatCampaignPerformanceCubeQuery(task=self.task)
      ),
    ]

class TikTokPerformanceCubeMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeTikTokTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=TikTokAdPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=TikTokAdGroupPerformanceCubeQuery(task=self.task)
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=TikTokCampaignPerformanceCubeQuery(task=self.task)
      ),
    ]

class PerformanceCubeAppsFlyerMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeAppsFlyerTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeAppsFlyerQuery(task=self.task)
      ),
      # The order of extrapolation is crucial to avoid compounding extrapolated data
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGranularityExtrapolationQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=PerformanceCubeEntity.campaign,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=PerformanceCubeTimeGranularity.day
        )
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGranularityExtrapolationQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=PerformanceCubeEntity.adset,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=PerformanceCubeTimeGranularity.day
        )
      ),
      PerformanceCubeMMPDeleteQuery(
        full_target_name=self.stage_table_name
      )
    ]

class PerformanceCubeAppsFlyerKeywordMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeAppsFlyerTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeAppsFlyerKeywordQuery(task=self.task)
      ),
    ]

class PerformanceCubeDataLockerMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeDataLockerTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeDataLockerQuery(task=self.task)
      ),
      # The order of extrapolation is crucial to avoid compounding extrapolated data
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGranularityExtrapolationQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=PerformanceCubeEntity.campaign,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=PerformanceCubeTimeGranularity.day
        )
      ),
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGranularityExtrapolationQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=PerformanceCubeEntity.adset,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=PerformanceCubeTimeGranularity.day
        )
      ),
      PerformanceCubeMMPDeleteQuery(
        full_target_name=self.stage_table_name
      )
    ]

class PerformanceCubeDataLockerKeywordMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeDataLockerKeywordTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeDataLockerKeywordQuery(task=self.task)
      ),
    ]

class PerformanceCubeDataLockerOrganicMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeDataLockerOrganicTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeDataLockerOrganicQuery(task=self.task)
      ),
    ]

class PerformanceCubeAdjustMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeAdjustTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      # The order of extrapolation is crucial to avoid compounding extrapolated data
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeAdjustMonthlyQuery(task=self.task)
      ),
      *[InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGranularityExtrapolationQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=e,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=PerformanceCubeTimeGranularity.month,
          dynamic_metric_count=self.task.dynamic_metric_count
        )
      ) for e in [PerformanceCubeEntity.campaign, PerformanceCubeEntity.adset]],
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeAdjustWeeklyQuery(task=self.task)
      ),
      *[InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGranularityExtrapolationQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=e,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=PerformanceCubeTimeGranularity.week,
          dynamic_metric_count=self.task.dynamic_metric_count
        )
      ) for e in [PerformanceCubeEntity.campaign, PerformanceCubeEntity.adset]],
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeAdjustDailyQuery(task=self.task)
      ),
      *[InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGranularityExtrapolationQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=e,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=PerformanceCubeTimeGranularity.day,
          dynamic_metric_count=self.task.dynamic_metric_count
        )
      ) for e in [PerformanceCubeEntity.campaign, PerformanceCubeEntity.adset]],
      PerformanceCubeMMPDeleteQuery(
        full_target_name=self.stage_table_name
      ),
    ]

class PerformanceCubeAdjustKeywordMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeAdjustKeywordTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      *[
        InsertQuery(
          full_target_name=self.stage_table_name,
          select_query=PerformanceCubeAdjustKeywordQuery(
            task=self.task,
            time_granularity=g,
          )
        )
        for g in PerformanceCubeTimeGranularity
      ],
      PerformanceCubeMMPDeleteQuery(
        full_target_name=self.stage_table_name
      ),
    ]

class PerformanceCubeGoogleAdsConversionActionMutator(PerformanceCubeUnfilteredBaseMutator[tasks.MaterializePerformanceCubeGoogleAdsConversionActionTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGoogleAdsConversionActionQuery(task=self.task)
      ),
      # The order of extrapolation is crucial to avoid compounding extrapolated data
      *[InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=PerformanceCubeGranularityExtrapolationQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=e,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=PerformanceCubeTimeGranularity.day
        )
      ) for e in [PerformanceCubeEntity.campaign, PerformanceCubeEntity.adset]],
      PerformanceCubeMMPDeleteQuery(
        full_target_name=self.stage_table_name
      )
    ]

class PerformanceCubeGlobalMutator(StagedQueryBaseMutator[tasks.MutatePerformanceCubeGlobalTask]):
  @property
  def is_row_count_query(self) -> bool:
    return True

  @property
  def stage_queries(self) -> List[SQLQuery]:
    return [
      PerformanceCubeGlobalTagResetQuery(
        task=self.task,
        full_target_name=self.task.report_table_model.full_table_name
      ),
      PerformanceCubeGlobalTagUpdateQuery(
        task=self.task,
        full_target_name=self.task.report_table_model.full_table_name,
        entity=PerformanceCubeEntity.ad
      ),
      PerformanceCubeGlobalTagUpdateQuery(
        task=self.task,
        full_target_name=self.task.report_table_model.full_table_name,
        entity=PerformanceCubeEntity.adset
      ),
      PerformanceCubeGlobalTagUpdateQuery(
        task=self.task,
        full_target_name=self.task.report_table_model.full_table_name,
        entity=PerformanceCubeEntity.campaign
      ),
    ]

  @property
  def query(self) -> SQLQuery:
    stage_query = super().query
    return SQLQuery(
      query=f'''
{stage_query.query}
select count(*) from {self.task.report_table_model.full_table_name};
      ''',
      substitution_parameters=stage_query.substitution_parameters
    )

class FilterPerformanceCubeBaseMutator(Generic[T], MaterializePerformanceCubeBaseMutator[T]):
  pass

class FilterPerformanceCubeChannelsMutator(FilterPerformanceCubeBaseMutator[tasks.FilterPerformanceCubeChannelsTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=FilterPerformanceCubeChannelsSelectQuery(task=self.task)
      ),
      FilterPerformanceCubeChannelsUpdateQuery(
        task=self.task,
        full_target_name=self.stage_table_name
      ),
      PerformanceCubeGranularityUpdateQuery(
        task=self.task,
        full_target_name=self.stage_table_name,
        entity_granularity=PerformanceCubeEntity.adset,
        cohort_granularity=PerformanceCubeCohortGranularity.daily,
        time_granularity=PerformanceCubeTimeGranularity.day,
        target_platform=self.task.target_channel_platform
      ),
      PerformanceCubeGranularityUpdateQuery(
        task=self.task,
        full_target_name=self.stage_table_name,
        entity_granularity=PerformanceCubeEntity.campaign,
        cohort_granularity=PerformanceCubeCohortGranularity.daily,
        time_granularity=PerformanceCubeTimeGranularity.day,
        target_platform=self.task.target_channel_platform
      ),
    ]

class FilterPerformanceCubeMMPsMutator(FilterPerformanceCubeBaseMutator[tasks.FilterPerformanceCubeMMPsTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=FilterPerformanceCubeMMPsSelectQuery(task=self.task)
      ),
      FilterPerformanceCubeMMPsMatchUpdateQuery(
        task=self.task,
        full_target_name=self.stage_table_name
      ),
      FilterPerformanceCubeMMPsUpdateQuery(
        task=self.task,
        full_target_name=self.stage_table_name
      ),
      FilterPerformanceCubeMMPsUnmatchedDeleteQuery(
        task=self.task,
        full_target_name=self.stage_table_name
      ),
      *[
        PerformanceCubeGranularityUpdateQuery(
          task=self.task,
          full_target_name=self.stage_table_name,
          entity_granularity=e,
          cohort_granularity=PerformanceCubeCohortGranularity.daily,
          time_granularity=t,
          dynamic_metric_count=self.task.dynamic_metric_count
        )
        for t in PerformanceCubeTimeGranularity
        for e in PerformanceCubeEntity
        if e is not PerformanceCubeEntity.ad or t is not PerformanceCubeTimeGranularity.day
      ],
      FilterPerformanceCubeMMPsDateDeleteQuery(
        task=self.task,
        full_target_name=self.stage_table_name
      ),
      FilterPerformanceCubeMMPsCrystallizationQuery(
        task=self.task,
        full_target_name=self.stage_table_name
      ),
    ]

class FilterPerformanceCubeOrganicMutator(FilterPerformanceCubeBaseMutator[tasks.FilterPerformanceCubeOrganicTask]):
  @property 
  def stage_queries(self) -> List[SQLQuery]:
    return [
      InsertQuery(
        full_target_name=self.stage_table_name,
        select_query=FilterPerformanceCubeOrganicSelectQuery(task=self.task)
      ),
      FilterPerformanceCubeOrganicUpdateQuery(
        task=self.task,
        full_target_name=self.stage_table_name
      ),
      FilterPerformanceCubeMMPsCrystallizationQuery(
        task=self.task,
        full_target_name=self.stage_table_name
      ),
    ]
