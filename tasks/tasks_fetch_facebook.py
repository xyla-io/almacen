import models
import sys, os

from . import base
from . import tasks_fetch_entity
from figaro import FacebookAPI
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.adactivity import AdActivity
from typing import Dict, List, Optional

class FetchFacebookReportTask(base.FetchReportTask):
  api: Optional[FacebookAPI]

  @property
  def debug_description(self) -> str:
    return '{}: {} ({}) — {}'.format(
      self.company_display_name,
      ', '.join(p.display_name for p in self.task_set.products),
      self.account_id,
      self.task_type.value,
    )

  @property
  def account_id(self) -> int:
    return self.task_set.config['account_id']

  @property
  def campaign_regex_filter(self) -> Optional[str]:
    return self.task_set.config['campaign_regex_filter'] if 'campaign_regex_filter' in self.task_set.config else None

  @property
  def infer_app_display_name(self) -> bool:
    return self.task_set.config['infer_app_display_name'] if 'infer_app_display_name' in self.task_set.config else False

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'account_id': self.account_id,
    }

class FetchFacebookCampaignsReportTask(FetchFacebookReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  campaigns: List[Campaign] = []
  ad_sets: List[AdSet] = []

  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_facebook_campaigns

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.FacebookCampaignReportTableModel(schema_name=self.report_table_schema)

  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      )
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='facebook',
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
    ]
  
  @property
  def default_fetch_columns(self) -> List[str]:
    return [
      'account_id',
      'account_name',
      'account_currency',
      'campaign_id',
      'campaign_name',
      'impressions',
      'clicks',
      'spend',
      'unique_clicks',
      'actions',
      'date_start',
      'date_stop',
    ]

class FetchFacebookAdSetsReportTask(FetchFacebookCampaignsReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_facebook_adsets
  
  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.FacebookAdSetReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='adset_id',
        source_value_column='adset_name',
        target_value_column='latest_adset_name'
      )
    ]
  
  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self, 'facebook-adgroup'),
      channel='facebook',
      entity='adgroup',
      name_column='adset_name',
      id_column='adset_id'
    )

  @property
  def default_fetch_columns(self) -> List[str]:
    return [
      'account_id',
      'account_name',
      'account_currency',
      'campaign_id',
      'campaign_name',
      'adset_id',
      'adset_name',
      'impressions',
      'clicks',
      'spend',
      'unique_clicks',
      'actions',
      'date_start',
      'date_stop',
      'frequency',
    ]

class FetchFacebookAdsReportTask(FetchFacebookCampaignsReportTask):
  account: Optional[AdAccount] = None
  ads: List[Ad] = []
  ad_creatives: List[AdCreative] = []
  ad_activity_history: List[AdActivity] = []

  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_facebook_ads
  
  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.FacebookAdReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self),
      channel='facebook',
      entity='ad',
      name_column='ad_name',
      id_column='ad_id'
    )

  @property
  def default_fetch_columns(self) -> List[str]:
    return [
      'account_id',
      'account_name',
      'account_currency',
      'ad_id',
      'ad_name',
      'adset_id',
      'adset_name',
      'campaign_id',
      'campaign_name',
      'actions',
      'frequency',
      'impressions',
      'reach',
      'social_spend',
      'spend',
      'clicks',
      'quality_ranking',
      'engagement_rate_ranking',
      'conversion_rate_ranking',
      'unique_clicks',
      'date_start',
      'date_stop'
    ]
