import pandas as pd
import tasks
import json

from . import base
from typing import TypeVar, Generic, List, Dict, Optional
from datetime import datetime

T = TypeVar(tasks.FetchSnapchatReportTask)
class SnapchatReportProcessor(Generic[T], base.ReportProcessor[T]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'ad_account_id': self.task.ad_account_id,
      'ad_account_timezone': self.task.api.ad_account['timezone'],
      'app_display_name': self.task.app_display_name,
      'converted_currency': self.task.currency,
      'platform': None,
      'os_types': None,
      'extracted_product': None,
      'extracted_product_id': None,
      'product': None,
      'product_id': None,
      'product_name': None,
      'product_platform': None,
      'product_os': None,
    }

  @property
  def null_column_defaults(self) -> Dict[str, any]:
    return {
      'platform': '',
    }
  
  def _convert_microcurrency_values_to_normal_currency(self, report: pd.DataFrame):
    for column in self.task.money_columns:
      report[column] = report[column].apply(lambda x: x / 1000000 if x is not None else None)
  
  def _get_targeting_data(self, ad_squad: Dict[str, any]) -> List[str]:
    if 'devices' not in ad_squad['targeting']:
      return []

    os_types = list({
      d['os_type'].lower()
      for d in ad_squad['targeting']['devices']
      if 'os_type' in d
    })
    return list(sorted(os_types))
  
  def add_product_data(self, report: pd.DataFrame):
    pass

  def add_entity_data(self):
    pass
  
  def process(self):
    report = self.task.report
    report.reset_index(drop=True, inplace=True)
    report['start_time'] = report['start_time'].apply(lambda d: datetime.strptime(d, self.task.report_table_model.date_format_string))
    report['end_time'] = report['end_time'].apply(lambda d: datetime.strptime(d, self.task.report_table_model.date_format_string))
    report['start_time'] = report['start_time'].apply(lambda dt: dt.replace(tzinfo=None))
    report['end_time'] = report['end_time'].apply(lambda dt: dt.replace(tzinfo=None))
    super().process()

    self.add_entity_data()
    self.add_product_data(report=report)
    self._convert_microcurrency_values_to_normal_currency(report=report)
    return 'Processed values:\n{}'.format(self.task.report.count())

class SnapchatCampaignsReportProcessor(SnapchatReportProcessor[tasks.FetchSnapchatCampaignsReportTask]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'campaign_name': None,
      'campaign_status': None,
      'campaign_objective': None,
      'campaign_daily_budget': None,
      'campaign_lifetime_spend_cap': None,
      'campaign_android_app_url': None,
      'campaign_ios_app_id': None,
    }
  
  def add_entity_data(self):
    for campaign in self.task.campaigns:
      self._add_campaign_data(report=self.task.report, campaign=campaign)
      self._add_campaign_platform_data(report=self.task.report, campaign=campaign)

  def add_product_data(self, report: pd.DataFrame):
    report['extracted_product_id'] = report.apply(lambda r: r.campaign_android_app_url if r.platform == 'android' else r.campaign_ios_app_id if r.platform == 'ios' else None, axis=1)
    report['product_id'] = report.extracted_product_id
    for index, row in report.loc[report.os_types == json.dumps(['android', 'ios'])].iterrows():
      products = {
        self.task.task_set.product_for_platform_id(platform_id=i)
        for i in [row.campaign_android_app_url, row.campaign_ios_app_id] 
        if i is not None
      }
      if len(products) == 1 and next(iter(products)) is not None:
        report.loc[index, ['extracted_product', 'product']] = next(iter(products)).identifier

    self.add_product_canonical_columns(report=report)

  def _add_campaign_data(self, report: pd.DataFrame, campaign: Dict[str, any]):
    report.loc[report.campaign_id == campaign['id'], 'campaign_name'] = campaign['name']
    report.loc[report.campaign_id == campaign['id'], 'campaign_status'] = campaign['status']
    report.loc[report.campaign_id == campaign['id'], 'campaign_objective'] = campaign['objective'] if 'objective' in campaign else None
    report.loc[report.campaign_id == campaign['id'], 'campaign_daily_budget'] = campaign['daily_budget_micro'] if 'daily_budget_micro' in campaign else None
    report.loc[report.campaign_id == campaign['id'], 'campaign_lifetime_spend_cap'] = campaign['lifetime_spend_cap_mico'] if 'lifetime_spend_cap_mico' in campaign else None
    if 'measurement_spec' in campaign:
      report.loc[report.campaign_id == campaign['id'], 'campaign_android_app_url'] = campaign['measurement_spec']['android_app_url'] if 'android_app_url' in campaign['measurement_spec'] else None
      report.loc[report.campaign_id == campaign['id'], 'campaign_ios_app_id'] = campaign['measurement_spec']['ios_app_id'] if 'ios_app_id' in campaign['measurement_spec'] else None
  
  def _add_campaign_platform_data(self, report: pd.DataFrame, campaign: Dict[str, any]):
    ad_squads = [squad for squad in self.task.ad_squads if squad['campaign_id'] == campaign['id']]
    os_types = list(sorted({o for ad_squad in ad_squads for o in self._get_targeting_data(ad_squad)}))
    report.loc[report.campaign_id == campaign['id'], 'os_types'] = json.dumps(os_types) if os_types else None
    report.loc[report.campaign_id == campaign['id'], 'platform'] = os_types[0] if len(os_types) == 1 and os_types[0] in ['android', 'ios'] else None

class SnapchatAdSquadsReportProcessor(SnapchatCampaignsReportProcessor):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'ad_squad_id': None,
      'ad_squad_name': None,
      'ad_squad_status': None,
      'ad_squad_placement': None,
      'ad_squad_optimization_goal': None,
      'ad_squad_daily_budget': None,
      'ad_squad_auto_bid': None,
      'ad_squad_bid': None,
      'ad_squad_billing_event': None,
    }
  
  def add_entity_data(self):
    self.task.report.ad_squad_id = self.task.report.adsquad_id
    self.task.report.drop(columns=['adsquad_id'], inplace=True)

    for ad_squad in self.task.ad_squads:
      self._add_ad_squad_data(report=self.task.report, ad_squad=ad_squad)
      self._add_ad_squad_platform_data(report=self.task.report, ad_squad=ad_squad)
    for campaign in self.task.campaigns:
      self._add_campaign_data(report=self.task.report, campaign=campaign)

  def _add_ad_squad_data(self, report: pd.DataFrame, ad_squad: Dict[str, any]):
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_name'] = ad_squad['name']
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_status'] = ad_squad['status']
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_placement'] = ad_squad['placement']
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_optimization_goal'] = ad_squad['optimization_goal']
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_daily_budget'] = ad_squad['daily_budget_micro'] if 'daily_budget_micro' in ad_squad else None
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_lifetime_budget'] = ad_squad['lifetime_budget_micro'] if 'lifetime_budgent_micro' in ad_squad else None
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_auto_bid'] = ad_squad['auto_bid'] if 'auto_bid' in ad_squad else None
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_bid'] = ad_squad['bid_micro'] if 'bid_micro' in ad_squad else None
    report.loc[report.ad_squad_id == ad_squad['id'], 'ad_squad_billing_event'] = ad_squad['billing_event']

  def _add_ad_squad_platform_data(self, report: pd.DataFrame, ad_squad: Dict[str, any]):
    os_types = self._get_targeting_data(ad_squad=ad_squad)
    report.loc[report.ad_squad_id == ad_squad['id'], 'os_types'] = json.dumps(os_types) if os_types else None
    report.loc[report.ad_squad_id == ad_squad['id'], 'platform'] = os_types[0] if len(os_types) == 1 and os_types[0] in ['android', 'ios'] else None


class SnapchatAdsReportProcessor(SnapchatAdSquadsReportProcessor):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'ad_name': None,
      'ad_status': None,
      'ad_type': None,
      'creative_id': None,
    }

  def add_entity_data(self):
    for ad in self.task.ads:
      self._add_ad_data(report=self.task.report, ad=ad)
    for ad_squad in self.task.ad_squads:
      self._add_ad_squad_data(report=self.task.report, ad_squad=ad_squad)
      self._add_ad_squad_platform_data(report=self.task.report, ad_squad=ad_squad)
    for campaign in self.task.campaigns:
      self._add_campaign_data(report=self.task.report, campaign=campaign)

  def _add_ad_data(self, report: pd.DataFrame, ad: Dict[str, any]):
    report.loc[report.ad_id == ad['id'], 'ad_name'] = ad['name']
    report.loc[report.ad_id == ad['id'], 'ad_squad_id'] = ad['ad_squad_id']
    report.loc[report.ad_id == ad['id'], 'ad_status'] = ad['status']
    report.loc[report.ad_id == ad['id'], 'ad_type'] = ad['type']
    report.loc[report.ad_id == ad['id'], 'creative_id'] = ad['creative_id']