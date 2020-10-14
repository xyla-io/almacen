import pandas as pd
import tasks
import json
import re

from . import base
from typing import TypeVar, Generic, List, Dict, Optional
from datetime import datetime

T = TypeVar(tasks.FetchTikTokReportTask)
class TikTokReportProcessor(Generic[T], base.ReportProcessor[T]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'advertiser_id': self.task.advertiser_id,
      'account_timezone': self.task.ad_account['timezone'],
      'account_currency': self.task.ad_account['currency'],
      'converted_currency': self.task.currency,
      'platform': None,
      'extracted_os_types': None,
      'extracted_product': None,
      'extracted_product_id': None,
      'extracted_product_os': None,
      'extracted_product_name': None,
      'product': None,
      'product_id': None,
      'product_name': None,
      'product_platform': None,
      'product_os': None,
    }
  
  @property
  def json_columns(self) -> List[str]:
    return [
      'extracted_os_types'
    ]

  def _get_os_types(self, adgroup: Dict[str, any]) -> List[str]:
    if 'operation_system' not in adgroup or not adgroup['operation_system']:
      return []

    os_types = [t.lower() for t in adgroup['operation_system']]
    return list(sorted(os_types))
  
  def _get_product_id(self, adgroup: Dict[str, any]) -> Optional[str]:
    if 'app_type' not in adgroup:
      return None
    
    url = adgroup['app_download_url']
    if adgroup['app_type'] == 'APP_IOS':
      match = re.match(r'^https?://(itunes|apps)\.apple\.com/.+/id(.+)$', url)
      return match[2] if match else None
    elif adgroup['app_type'] == 'APP_ANDROID':
      match = re.match(r'^https?://play\.google\.com/store/apps/details\?id=([^&]+)', url)
      return match[1] if match else None
    else:
      return None
  
  def _get_product_platform(self, adgroup: Dict[str, any]) -> Optional[str]:
    if 'app_type' not in adgroup:
      return None
    elif adgroup['app_type'] == 'APP_IOS':
      return 'ios'
    elif adgroup['app_type'] == 'APP_ANDROID':
      return 'android'
    else:
      return None
  
  def _get_product_name(self, adgroup: Dict[str, any]) -> Optional[str]:
    return adgroup['app_name'] if 'app_name' in adgroup else None

  def drop_entity_account_columns(self):
    self.task.report.drop(columns=f'{self.task.entity_level}_advertiser_id', inplace=True)

  def json_encode_necessary_columns(self):
    for column in self.json_columns:
      self.task.report[column] = self.task.report[column].apply(lambda v: json.dumps(v) if v else None)
  
  def find_common_product_for_platform_ids(self, product_ids: List[str]) -> Optional[str]:
    products = {
      self.task.task_set.product_for_platform_id(platform_id=i)
      for i in product_ids
      if i is not None
    }
    return next(iter(products)).identifier if len(products) == 1 and next(iter(products)) is not None else None
  
  def calculate_columns_for_adgroups(self, adgroups: List[Dict[str, any]], index: pd.Index):
    os_types = sorted({o for adgroup in adgroups for o in self._get_os_types(adgroup)})
    product_ids = sorted({self._get_product_id(a) for a in adgroups})
    product_platforms = sorted({self._get_product_platform(a) for a in adgroups})
    app_names = sorted({self._get_product_name(a) for a in adgroups})

    report = self.task.report
    report.loc[index, 'extracted_os_types'] = pd.Series(index=index, data=[os_types] * len(index)) if os_types else None
    report.loc[index, 'platform'] = os_types[0] if len(os_types) == 1 and os_types[0] in ['android', 'ios'] else None
    report.loc[index, ['extracted_product_id', 'product_id']] = product_ids[0] if len(product_ids) == 1 else None
    report.loc[index, ['extracted_product_os', 'product_os']] = product_platforms[0] if len(product_platforms) == 1 else None
    report.loc[index, ['extracted_product_name', 'product_name']] = app_names[0] if len(app_names) == 1 else None
    report.loc[index, ['extracted_product', 'product']] = self.find_common_product_for_platform_ids(product_ids)

  def add_calculated_columns(self):
    raise NotImplementedError()

  def process(self):
    report = self.task.report
    report.reset_index(drop=True, inplace=True)
    super().process()

    self.drop_entity_account_columns()
    self.add_calculated_columns()
    self.add_product_canonical_columns(report=self.task.report)
    self.json_encode_necessary_columns()
    return 'Processed values:\n{}'.format(self.task.report.count())

class TikTokCampaignsReportProcessor(TikTokReportProcessor[tasks.FetchTikTokCampaignsReportTask]):
  def filtered_adgroups(self, campaign_id: str) -> List[Dict[str, any]]:
    return [adgroup for adgroup in self.task.adgroups if adgroup['campaign_id'] == campaign_id]

  def add_calculated_columns(self):
    for campaign_id in self.task.report.campaign_campaign_id.unique():
      self.calculate_columns_for_adgroups(
        adgroups=self.filtered_adgroups(campaign_id=campaign_id),
        index=self.task.report.index[self.task.report.campaign_campaign_id == campaign_id]
      )

class TikTokAdGroupsReportProcessor(TikTokReportProcessor[tasks.FetchTikTokAdGroupsReportTask]):
  @property
  def json_columns(self) -> List[str]:
    return [
      *super().json_columns,
      'adgroup_placement',
      'adgroup_keywords',
      'adgroup_avatar_icon',
      'adgroup_audience',
      'adgroup_excluded_audience',
      'adgroup_location',
      'adgroup_age',
      'adgroup_languages',
      'adgroup_connection_type',
      'adgroup_operation_system',
      'adgroup_device_price',
      'adgroup_interest_category',
    ]

  def add_calculated_columns(self):
    for adgroup_id in self.task.report.adgroup_adgroup_id.unique():
      adgroup = list(filter(lambda a: a['adgroup_id'] == adgroup_id, self.task.adgroups))[0]
      self.calculate_columns_for_adgroups(
        adgroups=[adgroup],
        index=self.task.report.index[self.task.report.adgroup_adgroup_id == adgroup_id]
      )

class TikTokAdsReportProcessor(TikTokReportProcessor[tasks.FetchTikTokAdsReportTask]):
  @property
  def json_columns(self) -> List[str]:
    return [
      *super().json_columns,
      'ad_image_ids',
    ]

  def add_calculated_columns(self):
    for adgroup_id in self.task.report.ad_adgroup_id.unique():
      adgroup = list(filter(lambda a: a['adgroup_id'] == adgroup_id, self.task.adgroups))[0]
      self.calculate_columns_for_adgroups(
        adgroups=[adgroup],
        index=self.task.report.index[self.task.report.ad_adgroup_id == adgroup_id]
      )
