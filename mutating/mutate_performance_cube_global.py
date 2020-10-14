import tasks

from typing import TypeVar, Generic, List
from data_layer import SQLQuery
from .mutate_query_base import LatestSelectQuery, LatestUpdateQuery
from .mutate_performance_cube import StagedQueryBaseMutator
from .mutate_query_performance_cube import PerformanceCubeEntity, PerformanceCubeGlobalTagResetQuery, PerformanceCubeGlobalTagUpdateQuery

T = TypeVar(tasks.MutateReportTask)
class GlobalPerformanceCubeBaseMutator(Generic[T], StagedQueryBaseMutator[T]):
  @property
  def is_row_count_query(self) -> bool:
    return True

  @property
  def query(self) -> SQLQuery:
    staged_query = super().query
    return SQLQuery(
      query=f'''
{staged_query.query}
select count(*) from {self.task.report_table_model.full_table_name};
      ''',
      substitution_parameters=staged_query.substitution_parameters
    )

class TagsPerformanceCubeMutator(GlobalPerformanceCubeBaseMutator[tasks.MutatePerformanceCubeTagsTask]):
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

class LatestApplePerformanceCubeMutator(GlobalPerformanceCubeBaseMutator[tasks.MutatePerformanceCubeLatestAppleTask]):
  @property
  def stage_queries(self) -> List[SQLQuery]:
    return [
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_apple_search_ads_campaigns',
          date_column='date',
          value_column='campaignname',
          target_columns=['campaignid'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'campaignid': f'{PerformanceCubeEntity.campaign.value}_id',
          'campaignname': f'{PerformanceCubeEntity.campaign.value}_name',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Apple',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_apple_search_ads_adgroups',
          date_column='date',
          value_column='adgroupname',
          target_columns=['campaignid', 'adgroupid'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'campaignid': f'{PerformanceCubeEntity.campaign.value}_id',
          'adgroupid': f'{PerformanceCubeEntity.adset.value}_id',
          'adgroupname': f'{PerformanceCubeEntity.adset.value}_name',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Apple',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_apple_search_ads_creative_sets',
          date_column='date',
          value_column='creativesetname',
          target_columns=['campaignid', 'adgroupid', 'creativesetid'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'campaignid': f'{PerformanceCubeEntity.campaign.value}_id',
          'adgroupid': f'{PerformanceCubeEntity.adset.value}_id',
          'creativesetid': f'{PerformanceCubeEntity.ad.value}_id',
          'creativesetname': f'{PerformanceCubeEntity.ad.value}_name',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Apple',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_apple_search_ads_creative_sets',
          date_column='date',
          value_column='creativesetname',
          target_columns=['campaignid', 'adgroupid', 'creativesetid'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'campaignid': f'{PerformanceCubeEntity.campaign.value}_id',
          'adgroupid': f'{PerformanceCubeEntity.adset.value}_id',
          'creativesetid': f'{PerformanceCubeEntity.ad.value}_id',
          'creativesetname': f'{PerformanceCubeEntity.ad.value}_name',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Apple',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_apple_search_ads_keywords',
          date_column='date',
          value_column='keyword',
          target_columns=['campaignid', 'adgroupid', 'ad_id'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'campaignid': f'{PerformanceCubeEntity.campaign.value}_id',
          'adgroupid': f'{PerformanceCubeEntity.adset.value}_id',
          'keyword': f'{PerformanceCubeEntity.ad.value}_name',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Apple',))]
      ),
    ]

class LatestFacebookPerformanceCubeMutator(GlobalPerformanceCubeBaseMutator[tasks.MutatePerformanceCubeLatestFacebookTask]):
  @property
  def stage_queries(self) -> List[SQLQuery]:
    return [
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_facebook_campaigns',
          date_column='date_start',
          value_column=f'{PerformanceCubeEntity.campaign.value}_name',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_id'],
          exclude_values=['', None]
        ),
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Facebook',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_facebook_adsets',
          date_column='date_start',
          value_column=f'{PerformanceCubeEntity.adset.value}_name',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_id', f'{PerformanceCubeEntity.adset.value}_id'],
          exclude_values=['', None]
        ),
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Facebook',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_facebook_ads',
          date_column='date_start',
          value_column=f'{PerformanceCubeEntity.ad.value}_name',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_id', f'{PerformanceCubeEntity.adset.value}_id', f'{PerformanceCubeEntity.ad.value}_id'],
          exclude_values=['', None]
        ),
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Facebook',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_facebook_ads',
          date_column='date_start',
          value_column='date_start',
          filter_column='impressions',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_id', f'{PerformanceCubeEntity.adset.value}_id', f'{PerformanceCubeEntity.ad.value}_id'],
          exclude_values=[0, None],
          date_aggregator='min'
        ),
        source_to_target_columns={
          'date_start': 'first_impression_date',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Facebook',))]
      ),
    ]

class LatestGoogleAdsPerformanceCubeMutator(GlobalPerformanceCubeBaseMutator[tasks.MutatePerformanceCubeLatestGoogleAdsTask]):
  @property
  def stage_queries(self) -> List[SQLQuery]:
    return [
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_google_ads_campaigns',
          date_column='segments_date',
          value_column='campaign_name',
          target_columns=['campaign_id'],
          exclude_values=['', None]
        ),
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Google',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_google_ads_ads',
          date_column='segments_date',
          value_column='ad_display_name',
          target_columns=['ad_group_ad_ad_id'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'ad_group_ad_ad_id': f'{PerformanceCubeEntity.ad.value}_id',
          'ad_display_name': f'{PerformanceCubeEntity.ad.value}_name',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Google',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_google_ads_ads',
          date_column='segments_date',
          value_column='ad_asset_url',
          target_columns=['ad_group_ad_ad_id'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'ad_group_ad_ad_id': f'{PerformanceCubeEntity.ad.value}_id',
          'ad_asset_url': f'creative_image_url',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Google',))]
      ),
    ]

class LatestSnapchatPerformanceCubeMutator(GlobalPerformanceCubeBaseMutator[tasks.MutatePerformanceCubeLatestSnapchatTask]):
  @property
  def stage_queries(self) -> List[SQLQuery]:
    return [
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_snapchat_campaigns',
          date_column='start_time',
          value_column=f'{PerformanceCubeEntity.campaign.value}_name',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_id'],
          exclude_values=['', None]
        ),
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Snapchat',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_snapchat_adsquads',
          date_column='start_time',
          value_column=f'ad_squad_name',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_id', 'ad_squad_id'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'ad_squad_name': f'{PerformanceCubeEntity.adset.value}_name',
          'ad_squad_id': f'{PerformanceCubeEntity.adset.value}_id',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Snapchat',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_snapchat_ads',
          date_column='start_time',
          value_column=f'{PerformanceCubeEntity.ad.value}_name',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_id', 'ad_squad_id', f'{PerformanceCubeEntity.ad.value}_id'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'ad_squad_id': f'{PerformanceCubeEntity.adset.value}_id',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Snapchat',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_snapchat_ads',
          date_column='start_time',
          value_column='start_time',
          filter_column='impressions',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_id', 'ad_squad_id', f'{PerformanceCubeEntity.ad.value}_id'],
          exclude_values=[0, None],
          date_aggregator='min'
        ),
        source_to_target_columns={
          'start_time': 'first_impression_date',
          'ad_squad_id': f'{PerformanceCubeEntity.adset.value}_id',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('Snapchat',))]
      ),
    ]

class LatestTikTokPerformanceCubeMutator(GlobalPerformanceCubeBaseMutator[tasks.MutatePerformanceCubeLatestTikTokTask]):
  @property
  def stage_queries(self) -> List[SQLQuery]:
    return [
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_tiktok_campaigns',
          date_column='campaign_stat_datetime',
          value_column=f'{PerformanceCubeEntity.campaign.value}_{PerformanceCubeEntity.campaign.value}_name',
          target_columns=[f'{PerformanceCubeEntity.campaign.value}_{PerformanceCubeEntity.campaign.value}_id'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          f'{PerformanceCubeEntity.campaign.value}_{PerformanceCubeEntity.campaign.value}_name': f'{PerformanceCubeEntity.campaign.value}_name',
          f'{PerformanceCubeEntity.campaign.value}_{PerformanceCubeEntity.campaign.value}_id': f'{PerformanceCubeEntity.campaign.value}_id',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('TikTok',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_tiktok_adgroups',
          date_column='adgroup_stat_datetime',
          value_column=f'adgroup_adgroup_name',
          target_columns=['adgroup_adgroup_id'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          'adgroup_adgroup_name': f'{PerformanceCubeEntity.adset.value}_name',
          'adgroup_adgroup_id': f'{PerformanceCubeEntity.adset.value}_id',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('TikTok',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_tiktok_ads',
          date_column='ad_stat_datetime',
          value_column=f'{PerformanceCubeEntity.ad.value}_{PerformanceCubeEntity.ad.value}_name',
          target_columns=[f'{PerformanceCubeEntity.ad.value}_{PerformanceCubeEntity.ad.value}_id'],
          exclude_values=['', None]
        ),
        source_to_target_columns={
          f'{PerformanceCubeEntity.ad.value}_{PerformanceCubeEntity.ad.value}_name': f'{PerformanceCubeEntity.ad.value}_name',
          f'{PerformanceCubeEntity.ad.value}_{PerformanceCubeEntity.ad.value}_id': f'{PerformanceCubeEntity.ad.value}_id',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('TikTok',))]
      ),
      LatestUpdateQuery(
        full_target_name=self.task.report_table_model.full_table_name,
        latest_select_query=LatestSelectQuery(
          full_source_name=f'{self.task.schema_prefix}fetch_tiktok_ads',
          date_column='ad_stat_datetime',
          value_column='ad_stat_datetime',
          filter_column='ad_show_cnt',
          target_columns=[f'{PerformanceCubeEntity.ad.value}_{PerformanceCubeEntity.ad.value}_id'],
          exclude_values=[0, None],
          date_aggregator='min'
        ),
        source_to_target_columns={
          'ad_stat_datetime': 'first_impression_date',
          f'{PerformanceCubeEntity.ad.value}_{PerformanceCubeEntity.ad.value}_id': f'{PerformanceCubeEntity.ad.value}_id',
        },
        target_condition_queries=[SQLQuery(f'{self.task.report_table_model.full_table_name}.channel = %s', ('TikTok',))]
      ),
    ]
