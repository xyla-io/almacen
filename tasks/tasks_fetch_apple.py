import models
import sys, os

from . import base
from . import tasks_fetch_entity
from heathcliff import SearchAdsAPI
from typing import List, Dict, Optional

class FetchAppleReportTask(base.FetchReportTask):
  org_name: Optional[str] = None
  api: Optional[SearchAdsAPI]

  @property
  def debug_description(self) -> str:
    organization_name = self.org_name if self.org_name is not None else '[org_name not retrieved]'
    return '{}: {} ({}) — {}'.format(
      self.company_display_name,
      organization_name,
      self.org_id,
      self.task_type.value
    )

  @property
  def org_id(self) -> int:
    return self.task_set.config['org_id']

  @property
  def app_id_display_names(self) -> Dict[int, str]:
    return {
      p.platform_ids['ios']: p.display_name
      for p in self.task_set.products
    }

  @property
  def keep_empty_app_display_names(self) -> bool:
    return self.task_set.config['keep_empty_app_display_names'] if 'keep_empty_app_display_names' in self.task_set.config else False
  
  @property
  def app_ids(self) -> List[int]:
    return sorted(list(self.app_id_display_names.keys()))

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'org_id': self.org_id,
      'adamId': self.app_ids
    }

class FetchAppleCampaignsReportTask(FetchAppleReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_apple_search_ads_campaigns

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppleCampaignReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaignId',
        source_value_column='campaignName',
        target_value_column='latest_campaign_name'
      )
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='apple_search_ads',
      entity='campaign',
      name_column='campaignName',
      id_column='campaignId'
    )

  @property
  def fetched_currency_column(self) -> str:
    return 'original_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'avgCPA',
      'avgCPT',
      'dailyBudget',
      'localSpend',
      'totalBudget',
    ]

class FetchAppleAdGroupsReportTask(FetchAppleReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_apple_search_ads_adgroups

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppleAdGroupReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaignId',
        source_value_column='campaignName',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='adgroupId',
        source_value_column='adGroupName',
        target_value_column='latest_adgroup_name'
      )
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='apple_search_ads',
      entity='adgroup',
      name_column='adGroupName',
      id_column='adGroupId'
    )

  @property
  def fetched_currency_column(self) -> str:
    return 'original_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'avgCPA',
      'avgCPT',
      'localSpend',
      'cpaGoal',
      'defaultCpcBid'
    ]

class FetchAppleKeywordsReportTask(FetchAppleReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_apple_search_ads_keywords

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppleKeywordReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaignId',
        source_value_column='campaignName',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='adgroupId',
        source_value_column='adGroupName',
        target_value_column='latest_adgroup_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='keywordId',
        source_value_column='keyword',
        target_value_column='latest_keyword_name'
      ),
    ]

  @property
  def fetched_currency_column(self) -> str:
    return 'original_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'avgCPA',
      'avgCPT',
      'localSpend',
      'bidAmount'
    ]

class FetchAppleCreativeSetsReportTask(FetchAppleReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_apple_search_ads_creative_sets

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppleCreativeSetReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaignId',
        source_value_column='campaignName',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='creativeSetId',
        source_value_column='creativeSetName',
        target_value_column='latest_creative_set_name'
      )
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self, 'apple_search_ads-ad'),
      channel='apple_search_ads',
      entity='ad',
      name_column='creativeSetName',
      id_column='creativeSetId'
    )

  @property
  def fetched_currency_column(self) -> str:
    return 'original_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'avgCPA',
      'avgCPT',
      'localSpend',
    ]

class FetchAppleSearchTermsReportTask(FetchAppleReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_apple_search_ads_searchterms

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppleSearchTermReportTableModel(schema_name=self.report_table_schema)

  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaignId',
        source_value_column='campaignName',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='adgroupId',
        source_value_column='adGroupName',
        target_value_column='latest_adgroup_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='keywordId',
        source_value_column='keyword',
        target_value_column='latest_keyword_name'
      ),
    ]

  @property
  def fetched_currency_column(self) -> str:
    return 'original_currency'

  @property
  def money_columns(self) -> List[str]:
    return [
      'avgCPA',
      'avgCPT',
      'localSpend',
    ]