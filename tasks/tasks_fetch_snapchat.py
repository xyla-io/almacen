import models
import sys, os

from . import base
from . import tasks_fetch_entity
from azrael import SnapchatAPI
from typing import Dict, List, Optional

class FetchSnapchatReportTask(base.FetchReportTask):
  api: Optional[SnapchatAPI]
  campaigns: List[Dict[str, any]] = []
  ad_squads: List[Dict[str, any]] = []
  ads: List[Dict[str, any]] = []

  @property
  def debug_description(self) -> str:
    return '{}: {} ({}) — {}'.format(
      self.company_display_name,
      self.app_display_name,
      self.ad_account_id,
      self.task_type.value,
    )

  @property
  def ad_account_id(self) -> int:
    return self.task_set.config['ad_account_id']

  @property
  def app_display_name(self) -> Optional[str]:
    assert len(self.task_set.products) == 1
    return self.task_set.products[0].display_name

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'ad_account_id': self.ad_account_id,
    }
  
  @property
  def view_attribution_window(self) -> Optional[str]:
    config = self.task_set.config
    return config['view_attribution_window'] if 'view_attribution_window' in config else None
  
  @property
  def swipe_up_attribution_window(self) -> Optional[str]:
    config = self.task_set.config
    return config['swipe_up_attribution_window'] if 'swipe_up_attribution_window' in config else None

class FetchSnapchatCampaignsReportTask(FetchSnapchatReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_snapchat_campaigns

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.SnapchatCampaignReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='snapchat',
      entity='campaign',
      name_column='campaign_name',
      id_column='campaign_id'
    )

  @property
  def fetched_currency_column(self) -> str:
    return 'account_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'spend',
      'campaign_daily_budget',
      'campaign_lifetime_spend_cap',
    ]
  
  @property
  def default_fetch_columns(self) -> List[str]:
    return [
      'impressions',
      'swipes',
      'screen_time_millis',
      'quartile_1',
      'quartile_2',
      'quartile_3',
      'view_completion',
      'spend',
      'video_views',
      'frequency',
      'uniques',
      'swipe_up_percent'
    ]

class FetchSnapchatAdSquadsReportTask(FetchSnapchatReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_snapchat_adsquads

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.SnapchatAdSquadReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_name',
        target_value_column='latest_ad_squad_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_name',
        target_value_column='ad_squad_name',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_status',
        target_value_column='ad_squad_status',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_placement',
        target_value_column='ad_squad_placement',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_optimization_goal',
        target_value_column='ad_squad_optimization_goal',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_bid',
        target_value_column='ad_squad_bid',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_billing_event',
        target_value_column='ad_squad_billing_event',
        only_missing_values=[None, '']
      ),
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self, 'snapchat-adgroup'),
      channel='snapchat',
      entity='adgroup',
      name_column='ad_squad_name',
      id_column='ad_squad_id'
    )

  @property
  def fetched_currency_column(self) -> str:
    return 'account_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'spend',
      'ad_squad_daily_budget',
      'ad_squad_bid',
      'campaign_daily_budget',
      'campaign_lifetime_spend_cap',
    ]
  
  @property
  def default_fetch_columns(self) -> List[str]:
    return [
      'impressions',
      'swipes',
      'screen_time_millis',
      'quartile_1',
      'quartile_2',
      'quartile_3',
      'view_completion',
      'spend',
      'video_views',
      'frequency',
      'uniques',
      'swipe_up_percent'
    ]

class FetchSnapchatAdsReportTask(FetchSnapchatReportTask, tasks_fetch_entity.FetchAdReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_snapchat_ads

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.SnapchatAdReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_id',
        source_value_column='ad_name',
        target_value_column='ad_name',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_id',
        source_value_column='ad_squad_id',
        target_value_column='ad_squad_id',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_id',
        source_value_column='ad_status',
        target_value_column='ad_status',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_id',
        source_value_column='ad_type',
        target_value_column='ad_type',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_id',
        source_value_column='creative_id',
        target_value_column='creative_id',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_name',
        target_value_column='ad_squad_name',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_status',
        target_value_column='ad_squad_status',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_placement',
        target_value_column='ad_squad_placement',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_optimization_goal',
        target_value_column='ad_squad_optimization_goal',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_bid',
        target_value_column='ad_squad_bid',
        only_missing_values=[None, '']
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_squad_id',
        source_value_column='ad_squad_billing_event',
        target_value_column='ad_squad_billing_event',
        only_missing_values=[None, '']
      ),
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='snapchat',
      entity='ad',
      name_column='ad_name',
      id_column='ad_id'
    )

  @property
  def ad_id_column(self) -> str:
    return 'ad_id'

  @property
  def fetched_currency_column(self) -> str:
    return 'account_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'spend',
      'ad_squad_daily_budget',
      'ad_squad_bid',
      'campaign_daily_budget',
      'campaign_lifetime_spend_cap',
    ]
  
  @property
  def default_fetch_columns(self) -> List[str]:
    return [
      'impressions',
      'swipes',
      'screen_time_millis',
      'quartile_1',
      'quartile_2',
      'quartile_3',
      'view_completion',
      'spend',
      'video_views',
      'frequency',
      'uniques',
      'swipe_up_percent'
    ]