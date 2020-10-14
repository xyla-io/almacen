import tasks
import sqlalchemy as alchemy
import pandas as pd

from . import base
from typing import Dict, Optional

class GoogleAdsReportCollector(base.ReportCollector[tasks.FetchGoogleAdsReportTask]):
  @property
  def transform_data_frame(self) -> bool:
    return True

  def collect(self):
    super().collect()
    self.task.report = None

class GoogleAdsAssetsCustomerReportCollector(base.UpsertReportCollector[tasks.FetchGoogleAdsAssetsCustomerReportTask]):
  @property
  def column_type_transform_dictionary(self) -> Optional[Dict[str, any]]:
    return {
      'asset_image_asset_file_size': pd.Int64Dtype(),
      'asset_image_asset_full_size_height_pixels': pd.Int64Dtype(),
      'asset_image_asset_full_size_width_pixels': pd.Int64Dtype(),
    }

class GoogleAdsAdsCustomerReportCollector(GoogleAdsReportCollector):
  @property
  def column_type_transform_dictionary(self) -> Optional[Dict[str, any]]:
    return {
      'metrics_clicks': pd.Int64Dtype(),
      'metrics_impressions': pd.Int64Dtype(),
      'metrics_cost_micros': pd.Int64Dtype(),
    }

class GoogleAdsAdAssetsCustomerReportCollector(GoogleAdsReportCollector):
  @property
  def column_type_transform_dictionary(self) -> Optional[Dict[str, any]]:
    return {
      'metrics_clicks': pd.Int64Dtype(),
      'metrics_impressions': pd.Int64Dtype(),
      'metrics_cost_micros': pd.Int64Dtype(),
    }

class GoogleAdsAdGroupsCustomerReportCollector(GoogleAdsReportCollector):
  @property
  def column_type_transform_dictionary(self) -> Optional[Dict[str, any]]:
    return {
      'metrics_video_views': pd.Int64Dtype(),
      'metrics_clicks': pd.Int64Dtype(),
      'metrics_interactions': pd.Int64Dtype(),
      'metrics_impressions': pd.Int64Dtype(),
    }

class GoogleAdsCampaignsCustomerReportCollector(GoogleAdsReportCollector):
  @property
  def column_type_transform_dictionary(self) -> Optional[Dict[str, any]]:
    return {
      'metrics_video_views': pd.Int64Dtype(),
      'metrics_clicks': pd.Int64Dtype(),
      'metrics_interactions': pd.Int64Dtype(),
      'metrics_impressions': pd.Int64Dtype(),
    }