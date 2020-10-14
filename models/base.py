import sqlalchemy as alchemy

from sqlalchemy.ext.declarative import declarative_base
from data_layer import SQLLayer
from enum import Enum
from typing import Optional, List, Dict, Union, Type

ReportModelBase = declarative_base()

class NullPlaceholder(Enum):
  string = ''
  integer = 2**63 - 1
  date_string = '0001-01-01'

class TableModel:
  schema_name: Optional[str]
  declarative_base: any

  def __init__(self, schema_name: Optional[str]=None, declarative_base: any=ReportModelBase):
    self.schema_name = schema_name
    self.declarative_base = declarative_base

  @property
  def table(self) -> alchemy.Table:
    if self.full_table_name not in self.declarative_base.metadata.tables:
      self.define_table()
    return self.declarative_base.metadata.tables[self.full_table_name]

  @property
  def table_name(self) -> str:
    raise NotImplementedError()

  @property
  def full_table_name(self) -> str:
    return f'{self.schema_name}.{str(self.table_name)}' if self.schema_name is not None else str(self.table_name)

  def table_exists(self, sql_layer: SQLLayer) -> bool:
    return sql_layer.table_exists(table_name=self.table_name, schema_name=self.schema_name)

  def define_table(self):
    raise NotImplementedError()
  
class ReportTableModel(TableModel):
  @property
  def date_column_name(self) -> str:
    raise NotImplementedError()

  @property
  def crystallized_column_name(self) -> str:
    return 'crystallized'

  @property
  def date_format_string(self) -> Optional[str]:
    return '%Y-%m-%d'

class ReportAction(Enum):
  fetch = 'fetch'
  mutate = 'mutate'
  verify = 'verify'

class ReportTarget(Enum):
  currency_exchange = 'currency_exchange'
  google_ads = 'google_ads'
  apple_search_ads = 'apple_search_ads'
  appsflyer = 'appsflyer'
  appsflyer_data_locker = 'appsflyer_data_locker'
  adjust = 'adjust'
  facebook = 'facebook'
  snapchat = 'snapchat'
  tiktok = 'tiktok'
  performance_cube_unfiltered = 'performance_cube_unfiltered'
  performance_cube_filtered = 'performance_cube_filtered'

class ReportTaskType(Enum):
  fetch_currency_exchange_rates = 'fetch_currency_exchange_rates'
  fetch_base_currency_exchage_rates = 'fetch_base_currency_exchage_rates'

  fetch_appsflyer_purchase_events = 'fetch_appsflyer_purchase_events'
  fetch_appsflyer_install_events = 'fetch_appsflyer_install_events'
  fetch_appsflyer_custom_events = 'fetch_appsflyer_custom_events'
  fetch_appsflyer_data_locker = 'fetch_appsflyer_data_locker'
  fetch_appsflyer_data_locker_hour_part = 'fetch_appsflyer_data_locker_hour_part'

  fetch_adjust_deliverables = 'fetch_adjust_deliverables'
  fetch_adjust_events = 'fetch_adjust_events'
  fetch_adjust_cohorts_measures_daily = 'fetch_adjust_cohorts_measures_daily'
  fetch_adjust_cohorts_measures_weekly = 'fetch_adjust_cohorts_measures_weekly'
  fetch_adjust_cohorts_measures_monthly = 'fetch_adjust_cohorts_measures_monthly'

  fetch_apple_search_ads_campaigns = 'fetch_apple_search_ads_campaigns'
  fetch_apple_search_ads_adgroups = 'fetch_apple_search_ads_adgroups'
  fetch_apple_search_ads_keywords = 'fetch_apple_search_ads_keywords'
  fetch_apple_search_ads_creative_sets = 'fetch_apple_search_ads_creative_sets'
  fetch_apple_search_ads_searchterms = 'fetch_apple_search_ads_searchterms'

  fetch_facebook_campaigns = 'fetch_facebook_campaigns'
  fetch_facebook_adsets = 'fetch_facebook_adsets'
  fetch_facebook_ads = 'fetch_facebook_ads'

  fetch_google_ads_ad_conversion_actions = 'fetch_google_ads_ad_conversion_actions'
  fetch_google_ads_ad_conversion_actions_customer = 'fetch_google_ads_ad_conversion_actions_customer'
  fetch_google_ads_assets = 'fetch_google_ads_assets'
  fetch_google_ads_assets_customer = 'fetch_google_ads_assets_customer'
  fetch_google_ads_ad_assets = 'fetch_google_ads_ad_assets'
  fetch_google_ads_ad_assets_customer = 'fetch_google_ads_ad_assets_customer'
  fetch_google_ads_ads = 'fetch_google_ads_ads'
  fetch_google_ads_ads_customer = 'fetch_google_ads_ads_customer'
  fetch_google_ads_campaigns = 'fetch_google_ads_campaigns'
  fetch_google_ads_campaigns_customer = 'fetch_google_ads_campaigns_customer'
  fetch_google_ads_ad_groups = 'fetch_google_ads_ad_groups'
  fetch_google_ads_ad_groups_customer = 'fetch_google_ads_ad_groups_customer'

  fetch_snapchat_campaigns = 'fetch_snapchat_campaigns'
  fetch_snapchat_adsquads = 'fetch_snapchat_adsquads'
  fetch_snapchat_ads = 'fetch_snapchat_ads'

  fetch_tiktok_campaigns = 'fetch_tiktok_campaigns'
  fetch_tiktok_adgroups = 'fetch_tiktok_adgroups'
  fetch_tiktok_ads = 'fetch_tiktok_ads'
  
  materialize_entity_campaign = 'materialize_entity_campaign'
  materialize_entity_adset = 'materialize_entity_adset'
  materialize_entity_ad = 'materialize_entity_ad'

  materialize_performance_cube_apple_search_ads = 'materialize_performance_cube_apple_search_ads'
  materialize_performance_cube_facebook = 'materialize_performance_cube_facebook'
  materialize_performance_cube_google_ads = 'materialize_performance_cube_google_ads'
  materialize_performance_cube_snapchat = 'materialize_performance_cube_snapchat'
  materialize_performance_cube_tiktok = 'materialize_performance_cube_tiktok'
  materialize_performance_cube_appsflyer = 'materialize_performance_cube_appsflyer'
  materialize_performance_cube_appsflyer_keyword = 'materialize_performance_cube_appsflyer_keyword'
  materialize_performance_cube_data_locker = 'materialize_performance_cube_data_locker'
  materialize_performance_cube_data_locker_keyword = 'materialize_performance_cube_data_locker_keyword'
  materialize_performance_cube_data_locker_organic = 'materialize_performance_cube_data_locker_organic'
  materialize_performance_cube_adjust = 'materialize_performance_cube_adjust'
  materialize_performance_cube_adjust_keyword = 'materialize_performance_cube_adjust_keyword'
  materialize_performance_cube_google_ads_conversion_action = 'materialize_performance_cube_google_ads_conversion_action'

  mutate_performance_cube_global = 'mutate_performance_cube_global'
  mutate_performance_cube_tags = 'mutate_performance_cube_tags'
  mutate_filter_performance_cube_channels = 'mutate_filter_performance_cube_channels'
  mutate_filter_performance_cube_mmps = 'mutate_filter_performance_cube_mmps'
  mutate_filter_performance_cube_organic = 'mutate_filter_performance_cube_organic'

  latest_performance_cube_apple_search_ads = 'latest_performance_cube_apple_search_ads'
  latest_performance_cube_facebook = 'latest_performance_cube_facebook'
  latest_performance_cube_google_ads = 'latest_performance_cube_google_ads'
  latest_performance_cube_snapchat = 'latest_performance_cube_snapchat'
  latest_performance_cube_tiktok = 'latest_performance_cube_tiktok'

  verify_performance_cube_unfiltered_apple = 'verify_performance_cube_unfiltered_apple'

class ReportTaskBehaviorType(Enum):
  run_subtasks = 'run_subtasks'
  fetch_date = 'fetch_date'
  provide_credentials = 'provide_credentials'
  provide_api = 'provide_api'
  fetch_report = 'fetch_report'
  process = 'process'
  collect = 'collect'
  mutate = 'mutate'
  verify = 'verify'

class ReportTaskBehaviorSubType(Enum):
  standard = 'standard'
  replace = 'replace'
  edit = 'edit'
  mutate_latest_column_value = 'mutate_latest_column_value'
  mutate_currency_exchange = 'mutate_currency_exchange'
  mutate_parse_tags = 'mutate_parse_tags'
  before = 'before'
  after = 'after'

class ReportTaskBehavior:
  behavior_type: ReportTaskBehaviorType
  behavior_subtype: ReportTaskBehaviorSubType
  behavior_subtype_info: Optional[any]
  behavior_result: Optional[any] = None

  def __init__(self, behavior_type: ReportTaskBehaviorType, behavior_subtype: ReportTaskBehaviorSubType=ReportTaskBehaviorSubType.standard, behavior_subtype_info: Optional[any]=None):
    self.behavior_type = behavior_type
    self.behavior_subtype = behavior_subtype
    self.behavior_subtype_info = behavior_subtype_info

class TrackAction(Enum):
  reset = 'reset'