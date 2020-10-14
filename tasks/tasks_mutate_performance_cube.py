import models

from . import base
from datetime import timedelta
from typing import Dict

class MaterializePerformanceCubeBaseTask(base.MutateReportTask):
  @property
  def debug_description(self) -> str:
    return '{} — {}'.format(
      self.company_display_name,
      self.task_type.value,
    )

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'source': self.task_type.value,
    }

  @property
  def materialize_days(self) -> int:
    return 60

  @property
  def rematerialize_days(self) -> int:
    return 43
  
  @property
  def backfill_rematerialize_days(self) -> int:
    return 3
    
  @property
  def dynamic_metric_count(self) -> int:
    return self.task_set.config['dynamic_metric_count'] if 'dynamic_metric_count' in self.task_set.config else 0

class MaterializePerformanceCubeUnfilteredBaseTask(MaterializePerformanceCubeBaseTask):
  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.PerformanceCubeUnfilteredTableModel(schema_name=self.report_table_schema)

class MaterializePerformanceCubeAppleTask(MaterializePerformanceCubeUnfilteredBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_apple_search_ads

class MaterializePerformanceCubeFacebookTask(MaterializePerformanceCubeUnfilteredBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_facebook

class MaterializePerformanceCubeGoogleAdsTask(MaterializePerformanceCubeUnfilteredBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_google_ads

class MaterializePerformanceCubeSnapchatTask(MaterializePerformanceCubeUnfilteredBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_snapchat

class MaterializePerformanceCubeTikTokTask(MaterializePerformanceCubeUnfilteredBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_tiktok

class MaterializePerformanceCubeAppsFlyerTask(MaterializePerformanceCubeUnfilteredBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_appsflyer

  @property
  def cohort_anchor(self) -> models.PerformanceCubeCohortAnchor:
    return models.PerformanceCubeCohortAnchor(self.task_set.config['cohort_anchor']) if 'cohort_anchor' in self.task_set.config else models.PerformanceCubeCohortAnchor.attribution

class MaterializePerformanceCubeAppsFlyerKeywordTask(MaterializePerformanceCubeAppsFlyerTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_appsflyer_keyword

class MaterializePerformanceCubeDataLockerTask(MaterializePerformanceCubeAppsFlyerTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_data_locker

class MaterializePerformanceCubeDataLockerKeywordTask(MaterializePerformanceCubeAppsFlyerTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_data_locker_keyword

class MaterializePerformanceCubeDataLockerOrganicTask(MaterializePerformanceCubeAppsFlyerTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_data_locker_organic

class MaterializePerformanceCubeAdjustTask(MaterializePerformanceCubeUnfilteredBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_adjust

class MaterializePerformanceCubeAdjustKeywordTask(MaterializePerformanceCubeAdjustTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_adjust_keyword

class MaterializePerformanceCubeGoogleAdsConversionActionTask(MaterializePerformanceCubeUnfilteredBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_performance_cube_google_ads_conversion_action
  
  @property
  def use_event_value_as_revenue(self) -> bool:
    return self.task_set.config['use_event_value_as_revenue'] if 'use_event_value_as_revenue' in self.task_set.config else False

class MutatePerformanceCubeGlobalTask(base.MutateReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.mutate_performance_cube_global

  @property
  def debug_description(self) -> str:
    return '{} — {}'.format(
      self.company_display_name,
      self.task_type.value,
    )

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppsFlyerCubeTableModel(schema_name=self.report_table_schema)

class FilterPerformanceCubeBaseTask(MaterializePerformanceCubeBaseTask):
  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.PerformanceCubeFilteredTableModel(schema_name=self.report_table_schema)

class FilterPerformanceCubeChannelsTask(FilterPerformanceCubeBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.mutate_filter_performance_cube_channels

  @property
  def target_channel_platform(self) -> bool:
    return self.task_set.config['target_channel_platform'] if 'target_channel_platform' in self.task_set.config else False

class FilterPerformanceCubeMMPsTask(FilterPerformanceCubeBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.mutate_filter_performance_cube_mmps

  @property
  def infer_channel(self) -> bool:
    return self.task_set.config['infer_channel']

class FilterPerformanceCubeOrganicTask(FilterPerformanceCubeBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.mutate_filter_performance_cube_organic

class MutatePerformanceCubeGlobalsBaseTask(base.MutateReportTask):
  @property
  def debug_description(self) -> str:
    return '{} — {}'.format(
      self.company_display_name,
      self.task_type.value,
    )

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.PerformanceCubeFilteredTableModel(schema_name=self.report_table_schema)

class MutatePerformanceCubeTagsTask(MutatePerformanceCubeGlobalsBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.mutate_performance_cube_tags

class MutatePerformanceCubeLatestAppleTask(MutatePerformanceCubeGlobalsBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.latest_performance_cube_apple_search_ads

class MutatePerformanceCubeLatestFacebookTask(MutatePerformanceCubeGlobalsBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.latest_performance_cube_facebook

class MutatePerformanceCubeLatestGoogleAdsTask(MutatePerformanceCubeGlobalsBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.latest_performance_cube_google_ads

class MutatePerformanceCubeLatestSnapchatTask(MutatePerformanceCubeGlobalsBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.latest_performance_cube_snapchat

class MutatePerformanceCubeLatestTikTokTask(MutatePerformanceCubeGlobalsBaseTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.latest_performance_cube_tiktok
