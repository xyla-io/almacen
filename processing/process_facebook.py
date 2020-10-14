import tasks
import json
import itertools
import re
import pandas as pd

from . import base
from datetime import datetime, timedelta
from pytz import timezone
from typing import TypeVar, Generic, List, Dict, Set, Optional
from facebook_business.adobjects.ad import Ad
from config import CompanyConfiguration

T = TypeVar(tasks.FetchFacebookReportTask)
class FacebookReportProcessor(Generic[T], base.ReportProcessor[T]):
  def process(self):
    super().process()
    report = self.task.report
    report.date_stop = report.date_stop.apply(lambda d: datetime.strptime(d, self.task.report_table_model.date_format_string))

class FacebookCampaignsReportProcessor(FacebookReportProcessor[tasks.FetchFacebookCampaignsReportTask]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'account_id': self.task.account_id,
      'app_display_name': None,
      'converted_currency': self.task.currency,
      'campaign_objective': None,
      'platform': None,
      'user_os': None,
      'extracted_product': None,
      'extracted_product_id': None,
      'extracted_product_platform': None,
      'product': None,
      'product_id': None,
      'product_name': None,
      'product_platform': None,
      'product_os': None,
    }
    
  def add_product_data(self, report: pd.DataFrame):
    for campaign in self.task.campaigns:
      platforms = self.get_campaign_platforms(campaign=campaign)
      report.loc[report.campaign_id == campaign['id'], 'user_os'] = json.dumps(list(sorted(platforms))) if platforms else None
      platform = next(iter(platforms)) if len(platforms) == 1 else None
      if platform in ['android', 'ios']:
        report.loc[report.campaign_id == campaign['id'], 'platform'] = platform
      
      product_platforms = self.get_campaign_product_platforms(campaign=campaign)
      product_platform = next(iter(product_platforms)) if len(product_platforms) == 1 else None
      report.loc[report.campaign_id == campaign['id'], ['extracted_product_platform', 'product_platform']] = product_platform

      product_ids = self.get_campaign_product_ids(campaign=campaign)
      products = {self.task.task_set.product_for_platform_id(platform_id=i) for i in product_ids}
      if len(product_ids) == 1:
        report.loc[report.campaign_id == campaign['id'], ['extracted_product_id', 'product_id']] = next(iter(product_ids))
      elif len(products) == 1 and next(iter(products)) is not None:
        report.loc[report.campaign_id == campaign['id'], ['extracted_product', 'product']] = next(iter(products)).identifier
      
      products = set(filter(lambda p: p is not None, products))
      report.loc[report.campaign_id == campaign['id'], 'app_display_name'] = self.calculate_app_display_name(
        entity_products=products,
        entity_product_ids=product_ids
      )
  
  def process(self):
    super().process()
    report = self.task.report
    report.actions = report.actions.apply(lambda a: json.dumps(a) if isinstance(a, list) or not pd.isna(a) else a)

    for campaign in self.task.campaigns:
      report.loc[report.campaign_id == campaign['id'], 'campaign_objective'] = campaign['objective']

    self.add_product_data(report=report)
    self.add_product_canonical_columns(report=report)
      
  def get_campaign_platforms(self, campaign: any) -> Set[str]:
    return {
      s
      for a in self.task.ad_sets 
      if a['campaign_id'] == campaign['id']
      for s in self.get_ad_set_platforms(a)
    }

  def get_campaign_product_ids(self, campaign: any) -> Set[str]:
    return {
      self.get_ad_set_product_id(a)
      for a in self.task.ad_sets 
      if a['campaign_id'] == campaign['id']
    }

  def get_campaign_product_platforms(self, campaign: any) -> Set[str]:
    return {
      p
      for a in self.task.ad_sets
      if a['campaign_id'] == campaign['id']
      for p in self.get_ad_set_product_platforms(a)
    }

  def get_ad_set_product_id(self, ad_set: any) -> Optional[str]:
    if 'promoted_object' not in ad_set or 'object_store_url' not in ad_set['promoted_object']:
      return None
    match = re.match(r'^(http://play.google.com/store/apps/details\?id=|https://itunes.apple.com/app/id)(.+)$', ad_set['promoted_object']['object_store_url'])
    return match[2] if match else None

  def get_ad_set_platforms(self, ad_set: any) -> List[str]:
    if 'targeting' not in ad_set or 'user_os' not in ad_set['targeting']:
      return []
    return [
      s.split('_', 1)[0].lower()
      for s in ad_set['targeting']['user_os']
    ]
    
  def get_ad_set_product_platforms(self, ad_set: any) -> Set[str]:
    promoted_object_platforms = set()
    if 'promoted_object' not in ad_set:
      return promoted_object_platforms
    
    promoted_object = ad_set['promoted_object']
    if 'object_store_url' in promoted_object:
      promoted_object_platforms.add('mobile')
    if 'pixel_id' in promoted_object:
      promoted_object_platforms.add('web')

    return promoted_object_platforms

  def calculate_app_display_name(self, entity_products: List[CompanyConfiguration.Product]=[], entity_product_ids: List[str]=[]) -> str:
    if self.task.infer_app_display_name:
      return next(iter(entity_products)).display_name if len(entity_products) == 1 and len(entity_product_ids) == 1 else None
    else:
      return self.task.task_set.products[0].display_name if self.task.task_set.products else None

class FacebookAdSetsReportProcessor(FacebookCampaignsReportProcessor):
  def add_product_data(self, report: pd.DataFrame):
    for ad_set in self.task.ad_sets:
      platforms = self.get_ad_set_platforms(ad_set=ad_set)
      report.loc[report.adset_id == ad_set['id'], 'user_os'] = json.dumps(platforms) if platforms else None
      if len(platforms) == 1 and platforms[0] in ['android', 'ios']:
        report.loc[report.adset_id == ad_set['id'], 'platform'] = platforms[0]
      
      product_platforms = self.get_ad_set_product_platforms(ad_set=ad_set)
      product_platform = next(iter(product_platforms)) if len(product_platforms) == 1 else None
      report.loc[report.adset_id == ad_set['id'], ['extracted_product_platform', 'product_platform']] = product_platform

      product_id = self.get_ad_set_product_id(ad_set=ad_set)
      report.loc[report.adset_id == ad_set['id'], ['extracted_product_id', 'product_id']] = product_id
      product = self.task.task_set.product_for_platform_id(platform_id=product_id)
      report.loc[report.adset_id == ad_set['id'], ['extracted_product', 'product']] = product.identifier if product is not None else None
      report.loc[report.adset_id == ad_set['id'], 'app_display_name'] = self.calculate_app_display_name(
        entity_products=[product] if product is not None else [],
        entity_product_ids=[product_id] if product_id is not None else []
      )

class FacebookAdsReportProcessor(FacebookAdSetsReportProcessor):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      **{c: None for c in self.creative_columns},
    }

  @property
  def creative_properties(self) -> List[str]:
    return [
      'id',
      'title',
      'body',
      'image_url',
      'name',
      'thumbnail_url',
    ]

  @property
  def creative_columns(self) -> List[str]:
    return ['creative_' + p for p in self.creative_properties]

  def process(self):
    super().process()
    report = self.task.report

    for ad in self.task.ads:
      self.assign_historical_data_for_ad(ad=ad)
    
  def assign_historical_data_for_ad(self, ad: Ad):
    if 'creative' in ad:
      self.assign_creative_data_for_ad(ad=ad)

    tz = timezone(self.task.account['timezone_name'])
    creative_activities = [
      {
        **a,
        'event_time': datetime.strptime(a['event_time'], '%Y-%m-%dT%H:%M:%S%z').astimezone(tz=tz),
        'extra_data': json.loads(a['extra_data']),
      } 
      for a in self.task.ad_activity_history 
      if a['event_type'] == 'update_ad_creative' and a['object_id'] == ad['id']
    ]
    creative_activities.sort(key=lambda a: a['event_time'], reverse=True)

    for activity in creative_activities:
      prior_date = activity['event_time'].date() - timedelta(days=1) if activity['event_time'].hour < 12 else activity['event_time'].date()
      until = datetime.combine(prior_date, datetime.min.time())
      self.assign_creative_data_for_ad(ad=ad, until=until, historical_values_array=activity['extra_data']['old_value'])

  def assign_creative_data_for_ad(self, ad: str, until: Optional[datetime]=None, historical_values_array: List[str]=[]):
    historical_values = {
      'image_url': historical_values_array[0],
      'title': historical_values_array[1],
      'body': historical_values_array[2],
      'id': historical_values_array[3],
    } if len(historical_values_array) == 4 else {
      'id': historical_values_array[-1],
    } if historical_values_array else {}
    historical_values = {k: v if v else None for k, v in historical_values.items()}

    creative_id = historical_values['id'] if 'id' in historical_values else ad['creative']['id'] if 'creative' in ad else None
    if creative_id is None:
      return
    creative = next((c for c in self.task.ad_creatives if c['id'] == creative_id), None)

    creative_values = [
      historical_values[p] if p in historical_values else
      creative[p] if creative is not None and p in creative else None 
      for p in self.creative_properties
    ]

    report = self.task.report
    if until is None:
      report.loc[(report.ad_id == ad['id']), self.creative_columns] = creative_values
    else:
      report.loc[(report.ad_id == ad['id']) & (report.date_stop <= until), self.creative_columns] = creative_values
