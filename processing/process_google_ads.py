import pandas as pd
import tasks
import json

from . import base
from typing import TypeVar, Generic, Dict, Optional
from datetime import datetime

T = TypeVar(tasks.FetchGoogleAdsReportTask)
class GoogleAdsReportProcessor(Generic[T], base.ReportProcessor[T]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'converted_currency': self.task.currency,
      'extracted_product': None,
      'product': None,
      'product_id': None,
      'product_name': None,
      'product_platform': None,
      'product_os': None,
    }

  def process(self) -> Optional[any]:
    def _sanitized_column_name(name: str) -> str:
      return name.replace('#', '_')
    
    report = self.task.report
    report.rename(_sanitized_column_name, axis='columns', inplace=True)
    super().process()

class GoogleAdsAdConversionActionCustomerReportProcessor(GoogleAdsReportProcessor[tasks.FetchGoogleAdsAdConversionActionsCustomerReportTask]):
  def process(self) -> Optional[any]:
    super().process()
    report = self.task.report

    if 'segments_conversion_action' in report:
      report.segments_conversion_action = report.segments_conversion_action.fillna('None')

    if 'conversion_action_resource_name' in report:
      report.drop('conversion_action_resource_name', inplace=True, axis=1)

    if 'campaign_app_campaign_setting_app_id' in report:
      report['product_id'] = report.campaign_app_campaign_setting_app_id

    self.add_product_canonical_columns(report=report)

class GoogleAdsAssetsCustomerReportProcessor(Generic[T], base.ReportProcessor[T]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'fetch_date': self.task.run_date,
    }

  def process(self) -> Optional[any]:
    def _sanitized_column_name(name: str) -> str:
      return name.replace('#', '_')
    
    report = self.task.report
    report.rename(_sanitized_column_name, axis='columns', inplace=True)
    super().process()

class GoogleAdsAdsCustomerReportProcessor(GoogleAdsReportProcessor[tasks.FetchGoogleAdsCampaignsCustomerReportTask]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'ad_display_name': None,
    }

  def process(self) -> Optional[any]:
    super().process()
    report = self.task.report

    if 'campaign_app_campaign_setting_app_id' in report:
      report['product_id'] = report.campaign_app_campaign_setting_app_id

    self.add_product_canonical_columns(report=report)
    if 'metrics_cost_micros' in report:
      report['cost'] = report['metrics_cost_micros'].apply(lambda x: x / 1000000 if x is not None else None)

    report = self.set_ad_display_name(report=report)
    for column in report.columns:
      report[column] = report[column].apply(lambda v: json.dumps(v) if isinstance(v, list) else v)
    self.task.report = report

  def set_ad_display_name(self, report: pd.DataFrame) -> pd.DataFrame:
    def assign_from_list(df: pd.DataFrame, source: str, target: str):
      if source not in df:
        return

      df = df[pd.isna(df[target])]
      report.loc[df.index, target] = df[source].apply(lambda l: l[-1] if l else None)

    def assign_from_value(df: pd.DataFrame, source: str, target: str):
      if source not in df:
        return

      df = df[pd.isna(df[target])]
      report.loc[df.index, target] = df[source]

    def assign_ad_display_name(df: pd.DataFrame, ad_type: str):
      if ad_type == 'APP_AD':
        assign_from_list(report.loc[df.index], 'ad_group_ad_ad_app_ad_headlines', 'ad_display_name')
        assign_from_list(report.loc[df.index], 'ad_group_ad_ad_app_ad_descriptions', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_app_ad_mandatory_ad_text', 'ad_display_name')
      elif ad_type == 'EXPANDED_TEXT_AD':
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_expanded_text_ad_headline_part1', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_expanded_text_ad_description', 'ad_display_name')
      elif ad_type == 'TEXT_AD':
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_text_ad_headline', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_text_ad_description1', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_text_ad_description2', 'ad_display_name')
      elif ad_type == 'GMAIL_AD':
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_gmail_ad_marketing_image_headline', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_gmail_ad_marketing_image_description', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_gmail_ad_teaser_headline', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_gmail_ad_teaser_description', 'ad_display_name')
      elif ad_type == 'IMAGE_AD':
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_image_ad_name', 'ad_display_name')
      elif ad_type == 'VIDEO_AD':
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_video_ad_in_stream_action_headline', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_video_ad_out_stream_headline', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_video_ad_out_stream_description', 'ad_display_name')
      elif ad_type == 'LEGACY_RESPONSIVE_DISPLAY_AD':
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_legacy_responsive_display_ad_short_headline', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_legacy_responsive_display_ad_promo_text', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_legacy_responsive_display_ad_description', 'ad_display_name')
      elif ad_type == 'RESPONSIVE_DISPLAY_AD':
        assign_from_list(report.loc[df.index], 'ad_group_ad_ad_responsive_display_ad_headlines', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_responsive_display_ad_long_headline', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_responsive_display_ad_promo_text', 'ad_display_name')
      elif ad_type == 'RESPONSIVE_SEARCH_AD':
        assign_from_list(report.loc[df.index], 'ad_group_ad_ad_responsive_search_ad_headlines', 'ad_display_name')
        assign_from_list(report.loc[df.index], 'ad_group_ad_ad_responsive_search_ad_descriptions', 'ad_display_name')
      elif ad_type == 'SHOPPING_COMPARISON_LISTING_AD':
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_shopping_comparison_listing_ad_headline', 'ad_display_name')
      elif ad_type == 'CALL_ONLY_AD':
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_call_only_ad_headline1', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_call_only_ad_headline2', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_call_only_ad_description1', 'ad_display_name')
        assign_from_value(report.loc[df.index], 'ad_group_ad_ad_call_only_ad_description2', 'ad_display_name')

    report.reset_index(drop=True, inplace=True)
    if 'ad_group_ad_ad_name' in report:
      report['ad_display_name'] = report.ad_group_ad_ad_name

    grouped_report = report.groupby('ad_group_ad_ad_type')
    for ad_type, sub_report in grouped_report:
      assign_ad_display_name(sub_report, ad_type)

    return report

class GoogleAdsAdAssetsCustomerReportProcessor(GoogleAdsReportProcessor[tasks.FetchGoogleAdsAdAssetsCustomerReportTask]):
  def process(self) -> Optional[any]:
    super().process()
    report = self.task.report

    if 'campaign_app_campaign_setting_app_id' in report:
      report['product_id'] = report.campaign_app_campaign_setting_app_id

    self.add_product_canonical_columns(report=report)
    if 'metrics_cost_micros' in report:
      report['cost'] = report['metrics_cost_micros'].apply(lambda x: x / 1000000 if x is not None else None)

class GoogleAdsAdGroupsCustomerReportProcessor(GoogleAdsReportProcessor[tasks.FetchGoogleAdsAdGroupsCustomerReportTask]):
  def process(self) -> Optional[any]:
    super().process()
    report = self.task.report

    if 'campaign_app_campaign_setting_app_id' in report:
      report['product_id'] = report.campaign_app_campaign_setting_app_id

    self.add_product_canonical_columns(report=report)
    if 'metrics_cost_micros' in report:
      report['cost'] = report['metrics_cost_micros'].apply(lambda x: x / 1000000 if x is not None else None)

class GoogleAdsCampaignsCustomerReportProcessor(GoogleAdsReportProcessor[tasks.FetchGoogleAdsCampaignsCustomerReportTask]):
  def process(self) -> Optional[any]:
    super().process()
    report = self.task.report

    if 'campaign_app_campaign_setting_app_id' in report:
      report['product_id'] = report.campaign_app_campaign_setting_app_id

    self.add_product_canonical_columns(report=report)
    if 'metrics_cost_micros' in report:
      report['cost'] = report['metrics_cost_micros'].apply(lambda x: x / 1000000 if x is not None else None)