import tasks
import models

from . import base
from .mutate_query_base import UpdateQuery
from data_layer import SQLQuery
from typing import Optional, List, OrderedDict as OrderedDictType
from collections import OrderedDict

class GoogleAdsUpdateAdsFromAssetsQuery(UpdateQuery):
  task: tasks.FetchGoogleAdsAdsReportTask

  def __init__(self, task: tasks.FetchGoogleAdsAdsCustomerReportTask):
    self.task = task
    super().__init__(full_target_name=self.task.report_table_model.full_table_name)

  @property 
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('ad_display_name = coalesce(ad_display_name, a.asset_name, a.asset_text_asset_text)'),
      SQLQuery(
        query='ad_asset_url = coalesce(ad_asset_url, %s || a.asset_youtube_video_asset_youtube_video_id, a.asset_image_asset_full_size_url)',
        substitution_parameters=('https://www.youtube.com/watch?v=',)
      ),
    ]

  @property
  def from_queries(self) -> OrderedDictType[str, SQLQuery]:
    return OrderedDict([
      ('a', SQLQuery(f'{self.task.schema_prefix}{models.ReportTaskType.fetch_google_ads_assets.value}')),
    ])

  @property
  def condition_queries(self) -> List[SQLQuery]:
    def extract_last_element_text(column: str) -> str:
      return f'json_extract_array_element_text("{column}", json_array_length("{column}") - 1, true)'
    return [
      SQLQuery('segments_date >= %s', (SQLQuery.format_date(self.task.report_start_date),)),
      SQLQuery('segments_date <= %s', (SQLQuery.format_date(self.task.uncrystallized_report_end_date),)),
      SQLQuery(query=f'''
  a.asset_resource_name = case ad_group_ad_ad_type
    when %s then coalesce({", ".join(extract_last_element_text(c) for c in [
      "ad_group_ad_ad_app_ad_youtube_videos",
      "ad_group_ad_ad_app_ad_images", 
      "ad_group_ad_ad_app_ad_html5_media_bundles",
    ])})
    when %s then coalesce({", ".join(extract_last_element_text(c) for c in [
      "ad_group_ad_ad_app_engagement_ad_videos",
      "ad_group_ad_ad_app_engagement_ad_images", 
    ])})
    when %s then ad_group_ad_ad_display_upload_ad_media_bundle
    when %s then coalesce({", ".join(extract_last_element_text(c) for c in [
      "ad_group_ad_ad_responsive_display_ad_youtube_videos",
      "ad_group_ad_ad_responsive_display_ad_marketing_images", 
      "ad_group_ad_ad_responsive_display_ad_square_marketing_images", 
      "ad_group_ad_ad_responsive_display_ad_logo_images", 
      "ad_group_ad_ad_responsive_display_ad_square_logo_images", 
    ])})
    else null
    end 
      ''',
      substitution_parameters=('APP_AD', 'APP_ENGAGEMENT_AD', 'HTML5_UPLOAD_AD', 'RESPONSIVE_DISPLAY_AD')
      )
    ]

class GoogleAdsAdsCustomerMutator(base.QueryReportMutator[tasks.FetchGoogleAdsAdsCustomerReportTask]):
  @property 
  def query(self) -> Optional[SQLQuery]:
    return GoogleAdsUpdateAdsFromAssetsQuery(task=self.task)
