from __future__ import annotations
import models
import re

from . import base
from . import tasks_fetch_entity
from . import tasks_fetch_currency_exchange
from config import CompanyConfiguration
from typing import List, Dict, Optional
from hazel import GoogleAdsAPI

class FetchGoogleAdsReportTask(base.FetchReportTask):
  api: Optional[GoogleAdsAPI]

  @property
  def debug_description(self) -> str:
    return '{}: {} — {}'.format(
      self.company_display_name,
      self.customer_id,
      self.task_type.value
    )

  @property
  def login_customer_id(self) -> str:
    return self.task_set.config['login_customer_id']
  
  @property
  def customer_id(self) -> Optional[str]:
    config = self.task_set.config
    return config['customer_id'] if 'customer_id' in config else self.login_customer_id

  @property
  def empty_as_null(self) -> bool:
    return self.task_set.config['empty_as_null'] if 'empty_as_null' in self.task_set.config else True

class FetchGoogleAdsMultiCustomerReportTask(FetchGoogleAdsReportTask, tasks_fetch_entity.FetchCampaignReportTask):
  customer_ids: List[str] = []

  @property
  def exclude_customer_ids(self) -> List[str]:
    return self.task_set.config['exclude_customer_ids'] if 'exclude_customer_ids' in self.task_set.config else []

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'customer_id': self.customer_ids,
    }

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_date),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.provide_credentials),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.provide_api),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.before
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_report),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.run_subtasks),
      *[
        models.ReportTaskBehavior(
          behavior_type=models.ReportTaskBehaviorType.mutate,
          behavior_subtype=models.ReportTaskBehaviorSubType.mutate_latest_column_value,
          behavior_subtype_info=s
        )
        for s in self.latest_value_mutation_specifiers
      ],
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]
  
  def generate_subtasks(self) -> List[base.ReportTask]:
    return [
      self.subtask_for_customer(customer_id=c)
      for c in self.customer_ids
    ]

  def subtask_for_customer(self, customer_id: int) -> FetchGoogleAdsCustomerReportTask:
    raise NotImplementedError()

class FetchGoogleAdsCustomerReportTask(base.FetchReportTask):
  api: GoogleAdsAPI
  customer_id: str

  def __init__(self, task_set: CompanyConfiguration.TaskSet, identifier_prefix: str, api: GoogleAdsAPI, customer_id: str):
    super().__init__(task_set=task_set, identifier_prefix=identifier_prefix)
    self.api = api
    self.customer_id = customer_id

  @property
  def debug_description(self) -> str:
    return '{}: {} — {}'.format(
      self.company_display_name,
      self.customer_id,
      self.task_type.value
    )

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'customer_id': self.customer_id,
    }

  @property
  def empty_as_null(self) -> bool:
    return self.task_set.config['empty_as_null'] if 'empty_as_null' in self.task_set.config else True

  @property
  def fetched_currency_column(self) -> str:
    return 'customer_currency_code'

  @property
  def money_columns(self) -> List[str]:
    return []

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return None

  def generate_subtasks(self) -> List[base.ReportTask]:
    if 'currency_exchange' not in self.task_set.config or not self.money_columns:
      return []
    return [
      tasks_fetch_currency_exchange.FetchCurrencyExchangeReportTask(
        task_set=self.task_set,
        identifier_prefix=self.identifier
      )
    ]

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.run_subtasks),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_date),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.before
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_report),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.process),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.process,
        behavior_subtype=models.ReportTaskBehaviorSubType.edit
      ),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate, 
        behavior_subtype=models.ReportTaskBehaviorSubType.replace
      ),
      *([models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate,
        behavior_subtype=models.ReportTaskBehaviorSubType.mutate_parse_tags,
        behavior_subtype_info=self.parse_tags_mutation_specifier
      )] if self.parse_tags_mutation_specifier else []),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.collect),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate,
        behavior_subtype=models.ReportTaskBehaviorSubType.mutate_currency_exchange
      ),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]

class FetchGoogleAdsAdConversionActionsReportTask(FetchGoogleAdsMultiCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_ad_conversion_actions

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAdConversionActionReportTableModel(schema_name=self.report_table_schema)

  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_ad_ad_id',
        source_value_column='ad_group_ad_ad_name',
        target_value_column='latest_ad_group_ad_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_id',
        source_value_column='ad_group_name',
        target_value_column='latest_ad_group_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='customer_id',
        source_value_column='customer_descriptive_name',
        target_value_column='latest_customer_descriptive_name'
      )
    ]

  @property
  def fetched_currency_column(self) -> str:
    return 'customer_currency_code'
  
  def subtask_for_customer(self, customer_id: int) -> FetchGoogleAdsCustomerReportTask:
    return FetchGoogleAdsAdConversionActionsCustomerReportTask(
      task_set=self.task_set,
      identifier_prefix=f'{self.identifier}.{customer_id}',
      api=self.api,
      customer_id=customer_id
    )

class FetchGoogleAdsAdConversionActionsCustomerReportTask(FetchGoogleAdsCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_ad_conversion_actions_customer

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAdConversionActionReportTableModel(schema_name=self.report_table_schema)

  @property
  def currency(self) -> str:
    return self.task_set.company_metadata.currency

class FetchGoogleAdsAssetsReportTask(FetchGoogleAdsMultiCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_assets

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAssetReportTableModel(schema_name=self.report_table_schema)

  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='customer_id',
        source_value_column='customer_descriptive_name',
        target_value_column='latest_customer_descriptive_name'
      )
    ]

  @property
  def fetched_currency_column(self) -> str:
    return 'customer_currency_code'

  @property
  def money_columns(self) -> List[str]:
    return []
  
  def subtask_for_customer(self, customer_id: int) -> FetchGoogleAdsCustomerReportTask:
    return FetchGoogleAdsAssetsCustomerReportTask(
      task_set=self.task_set,
      identifier_prefix=f'{self.identifier}.{customer_id}',
      api=self.api,
      customer_id=customer_id
    )

class FetchGoogleAdsAssetsCustomerReportTask(FetchGoogleAdsCustomerReportTask, base.UpsertReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_assets_customer

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAssetReportTableModel(schema_name=self.report_table_schema)

  @property
  def currency(self) -> str:
    return self.task_set.company_metadata.currency

  @property
  def merge_column_names(self) -> List[str]:
    return [
      'asset_resource_name'
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self, 'google_ads-asset'),
      channel='google_ads',
      entity='asset',
      name_column='asset_name',
      id_column='asset_id'
    )

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_date),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.before
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_report),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.process),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.process,
        behavior_subtype=models.ReportTaskBehaviorSubType.edit
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.collect),
      *([models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate,
        behavior_subtype=models.ReportTaskBehaviorSubType.mutate_parse_tags,
        behavior_subtype_info=self.parse_tags_mutation_specifier
      )] if self.parse_tags_mutation_specifier else []),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]

class FetchGoogleAdsAdsReportTask(FetchGoogleAdsMultiCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_ads

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAdReportTableModel(schema_name=self.report_table_schema)

  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_ad_ad_id',
        source_value_column='ad_group_ad_ad_name',
        target_value_column='latest_ad_group_ad_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_id',
        source_value_column='ad_group_name',
        target_value_column='latest_ad_group_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='customer_id',
        source_value_column='customer_descriptive_name',
        target_value_column='latest_customer_descriptive_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_ad_ad_id',
        source_value_column='ad_display_name',
        target_value_column='latest_ad_display_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_ad_ad_id',
        source_value_column='ad_asset_url',
        target_value_column='latest_ad_asset_url'
      ),
    ]

  @property
  def fetched_currency_column(self) -> str:
    return 'customer_currency_code'

  @property
  def money_columns(self) -> List[str]:
    return [
      'metrics_cost_micros',
      'cost'
    ]
  
  def subtask_for_customer(self, customer_id: int) -> FetchGoogleAdsCustomerReportTask:
    return FetchGoogleAdsAdsCustomerReportTask(
      task_set=self.task_set,
      identifier_prefix=f'{self.identifier}.{customer_id}',
      api=self.api,
      customer_id=customer_id
    )

class FetchGoogleAdsAdsCustomerReportTask(FetchGoogleAdsCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_ads_customer

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAdReportTableModel(schema_name=self.report_table_schema)

  @property
  def currency(self) -> str:
    return self.task_set.company_metadata.currency

  @property
  def money_columns(self) -> List[str]:
    return [
      'metrics_cost_micros',
      'cost'
    ]

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      *[
        b for b in super().generate_behaviors()
        if b.behavior_type is not models.ReportTaskBehaviorType.verify or b.behavior_subtype is not models.ReportTaskBehaviorSubType.after
      ],
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.mutate),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]

class FetchGoogleAdsAdAssetsReportTask(FetchGoogleAdsMultiCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_ad_assets

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAdAssetReportTableModel(schema_name=self.report_table_schema)

  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='asset_id',
        source_value_column='asset_name',
        target_value_column='latest_asset_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_ad_ad_id',
        source_value_column='ad_group_ad_ad_name',
        target_value_column='latest_ad_group_ad_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_id',
        source_value_column='ad_group_name',
        target_value_column='latest_ad_group_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='customer_id',
        source_value_column='customer_descriptive_name',
        target_value_column='latest_customer_descriptive_name'
      ),
    ]

  @property
  def fetched_currency_column(self) -> str:
    return 'customer_currency_code'

  @property
  def money_columns(self) -> List[str]:
    return [
      'metrics_cost_micros',
      'cost'
    ]
  
  def subtask_for_customer(self, customer_id: int) -> FetchGoogleAdsCustomerReportTask:
    return FetchGoogleAdsAdAssetsCustomerReportTask(
      task_set=self.task_set,
      identifier_prefix=f'{self.identifier}.{customer_id}',
      api=self.api,
      customer_id=customer_id
    )

class FetchGoogleAdsAdAssetsCustomerReportTask(FetchGoogleAdsCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_ad_assets_customer

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAdAssetReportTableModel(schema_name=self.report_table_schema)

  @property
  def currency(self) -> str:
    return self.task_set.company_metadata.currency

  @property
  def money_columns(self) -> List[str]:
    return [
      'metrics_cost_micros',
      'cost'
    ]

class FetchGoogleAdsAdGroupsReportTask(FetchGoogleAdsMultiCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_ad_groups

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAdGroupReportTableModel(schema_name=self.report_table_schema)

  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='ad_group_id',
        source_value_column='ad_group_name',
        target_value_column='latest_ad_group_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='customer_id',
        source_value_column='customer_descriptive_name',
        target_value_column='latest_customer_descriptive_name'
      )
    ]

  @property
  def fetched_currency_column(self) -> str:
    return 'customer_currency_code'

  @property
  def money_columns(self) -> List[str]:
    return [
      'campaign_target_cpa_target_cpa_micros',
      'ad_group_target_cpa_micros',
      'ad_group_target_cpm_micros',
      'metrics_cost_micros',
      'cost'
    ]
  
  def subtask_for_customer(self, customer_id: int) -> FetchGoogleAdsCustomerReportTask:
    return FetchGoogleAdsAdGroupsCustomerReportTask(
      task_set=self.task_set,
      identifier_prefix=f'{self.identifier}.{customer_id}',
      api=self.api,
      customer_id=customer_id
    )

class FetchGoogleAdsAdGroupsCustomerReportTask(FetchGoogleAdsCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_ad_groups_customer

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsAdGroupReportTableModel(schema_name=self.report_table_schema)

  @property
  def currency(self) -> str:
    return self.task_set.company_metadata.currency

  @property
  def money_columns(self) -> List[str]:
    return [
      'campaign_target_cpa_target_cpa_micros',
      'ad_group_target_cpa_micros',
      'ad_group_target_cpm_micros',
      'metrics_cost_micros',
      'cost'
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self, 'google_ads-adgroup'),
      channel='google_ads',
      entity='adgroup',
      name_column='ad_group_name',
      id_column='ad_group_id'
    )

class FetchGoogleAdsCampaignsReportTask(FetchGoogleAdsMultiCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_campaigns

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsCampaignReportTableModel(schema_name=self.report_table_schema)

  @property
  def latest_value_mutation_specifiers(self) -> List[Dict[str, any]]:
    return [
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='campaign_id',
        source_value_column='campaign_name',
        target_value_column='latest_campaign_name'
      ),
      tasks_fetch_entity.LatestValueMutationSpecifier(
        identifier_column='customer_id',
        source_value_column='customer_descriptive_name',
        target_value_column='latest_customer_descriptive_name'
      )
    ]

  @property
  def fetched_currency_column(self) -> str:
    return 'customer_currency_code'

  @property
  def money_columns(self) -> List[str]:
    return [
      'campaign_budget_total_amount_micros',
      'campaign_target_cpa_target_cpa_micros',
      'metrics_cost_micros',
      'campaign_budget_amount_micros',
      'cost'
    ]
  
  def subtask_for_customer(self, customer_id: int) -> FetchGoogleAdsCustomerReportTask:
    return FetchGoogleAdsCampaignsCustomerReportTask(
      task_set=self.task_set,
      identifier_prefix=f'{self.identifier}.{customer_id}',
      api=self.api,
      customer_id=customer_id
    )

class FetchGoogleAdsCampaignsCustomerReportTask(FetchGoogleAdsCustomerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_google_ads_campaigns_customer

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.GoogleAdsCampaignReportTableModel(schema_name=self.report_table_schema)

  @property
  def currency(self) -> str:
    return self.task_set.company_metadata.currency

  @property
  def money_columns(self) -> List[str]:
    return [
      'campaign_budget_total_amount_micros',
      'campaign_target_cpa_target_cpa_micros',
      'metrics_cost_micros',
      'campaign_budget_amount_micros',
      'cost'
    ]

  @property
  def parse_tags_mutation_specifier(self) -> Optional[tasks_fetch_entity.ParseTagsMutationSpecifier]:
    return tasks_fetch_entity.ParseTagsMutationSpecifier(
      parser=tasks_fetch_entity.ParseTagsMutationSpecifier.task_parser_name(self, 'google_ads-campaign'),
      channel='google_ads',
      entity='campaign',
      name_column='campaign_name',
      id_column='campaign_id'
    )
