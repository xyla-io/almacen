import models
import sys, os

from . import base
from . import tasks_fetch_entity
from lilu import TikTokAPI
from typing import Dict, List, Optional

class FetchTikTokReportTask(base.FetchReportTask):
  api: Optional[TikTokAPI]
  ad_account: Optional[Dict[str, any]]
  campaigns: List[Dict[str, any]] = []
  adgroups: List[Dict[str, any]] = []
  ads: List[Dict[str, any]] = []

  @property
  def debug_description(self) -> str:
    return '{}: {} ({}) — {}'.format(
      self.company_display_name,
      self.app_display_name,
      self.advertiser_id,
      self.task_type.value,
    )

  @property
  def advertiser_id(self) -> int:
    return self.task_set.config['advertiser_id']

  @property
  def app_display_name(self) -> Optional[str]:
    assert len(self.task_set.products) == 1
    return self.task_set.products[0].display_name

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'advertiser_id': self.advertiser_id,
    }

  @property
  def empty_as_null(self) -> bool:
    return True
  
  @property
  def entity_level(self) -> str:
    raise NotImplementedError()

class FetchTikTokCampaignsReportTask(FetchTikTokReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_tiktok_campaigns

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.TikTokCampaignReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_campaign_id',
        source_value_column='campaign_campaign_name',
        target_value_column='latest_campaign_name'
      ),
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='tiktok',
      entity='campaign',
      name_column='campaign_campaign_name',
      id_column='campaign_campaign_id'
    )

  @property
  def fetched_currency_column(self) -> str:
    return 'account_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'campaign_active_pay_amount',
      'campaign_active_pay_avg_amount',
      'campaign_active_pay_cost',
      'campaign_active_register_click_cost',
      'campaign_active_register_show_cost',
      'campaign_conversion_cost',
      'campaign_active_click_cost',
      'campaign_stat_cost',
      'campaign_active_pay_click_cost',
      'campaign_active_register_cost',
      'campaign_activate_cost',
      'campaign_active_pay',
      'campaign_active_cost',
      'campaign_active_pay_show_cost',
      'campaign_active_show_cost',
    ]
  
  @property
  def default_fetch_columns(self) -> List[str]:
    return None
  
  @property
  def entity_level(self) -> str:
    return 'campaign'

class FetchTikTokAdGroupsReportTask(FetchTikTokReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_tiktok_adgroups

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.TikTokAdGroupReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='adgroup_adgroup_id',
        source_value_column='adgroup_adgroup_name',
        target_value_column='latest_adgroup_name'
      ),
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='tiktok',
      entity='adgroup',
      name_column='adgroup_adgroup_name',
      id_column='adgroup_adgroup_id'
    )

  @property
  def fetched_currency_column(self) -> str:
    return 'account_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'adgroup_active_pay_amount',
      'adgroup_active_pay_avg_amount',
      'adgroup_active_pay_cost',
      'adgroup_active_pay_show',
      'adgroup_active_pay_click_cost',
      'adgroup_active_pay_click',
      'adgroup_active_pay',
      'adgroup_active_pay_show_cost',
      'adgroup_active_register_click_cost',
      'adgroup_active_register_show_cost',
      'adgroup_conversion_cost',
      'adgroup_active_click_cost',
      'adgroup_stat_cost',
      'adgroup_active_register_cost',
      'adgroup_click_cost',
      'adgroup_activate_cost',
      'adgroup_active_cost',
      'adgroup_active_show_cost',
    ]
  
  @property
  def default_fetch_columns(self) -> List[str]:
    return None
  
  @property
  def entity_level(self) -> str:
    return 'adgroup'

class FetchTikTokAdsReportTask(FetchTikTokReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_tiktok_ads

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.TikTokAdReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_ad_id',
        source_value_column='ad_ad_name',
        target_value_column='latest_ad_name'
      ),
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='tiktok',
      entity='ad',
      name_column='ad_ad_name',
      id_column='ad_ad_id'
    )

  @property
  def fetched_currency_column(self) -> str:
    return 'account_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'ad_active_pay_amount',
      'ad_active_pay_avg_amount',
      'ad_active_pay_cost',
      'ad_active_pay_show',
      'ad_active_pay_click_cost',
      'ad_active_pay_click',
      'ad_active_pay',
      'ad_active_pay_show_cost',
      'ad_active_register_click_cost',
      'ad_active_register_show_cost',
      'ad_conversion_cost',
      'ad_active_click_cost',
      'ad_stat_cost',
      'ad_active_register_cost',
      'ad_click_cost',
      'ad_activate_cost',
      'ad_active_cost',
      'ad_active_show_cost',
    ]
  
  @property
  def default_fetch_columns(self) -> List[str]:
    return None
  
  @property
  def entity_level(self) -> str:
    return 'ad'