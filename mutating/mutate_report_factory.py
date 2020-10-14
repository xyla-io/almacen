import tasks
import models

from . import base, mutate_appsflyer, mutate_performance_cube, mutate_performance_cube_global, mutate_google_ads, mutate_entity, mutate_tag

def report_mutator(task: tasks.ReportTask, behavior: models.ReportTaskBehavior) -> base.ReportMutator:
  if behavior.behavior_subtype is models.ReportTaskBehaviorSubType.standard:
    if task.task_type is models.ReportTaskType.materialize_performance_cube_apple_search_ads:
      return mutate_performance_cube.ApplePerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_facebook:
      return mutate_performance_cube.FacebookPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_google_ads:
      return mutate_performance_cube.GoogleAdsPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_snapchat:
      return mutate_performance_cube.SnapchatPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_tiktok:
      return mutate_performance_cube.TikTokPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_appsflyer:
      return mutate_performance_cube.PerformanceCubeAppsFlyerMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_appsflyer_keyword:
      return mutate_performance_cube.PerformanceCubeAppsFlyerKeywordMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_data_locker:
      return mutate_performance_cube.PerformanceCubeDataLockerMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_data_locker_keyword:
      return mutate_performance_cube.PerformanceCubeDataLockerKeywordMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_data_locker_organic:
      return mutate_performance_cube.PerformanceCubeDataLockerOrganicMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_adjust:
      return mutate_performance_cube.PerformanceCubeAdjustMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_adjust_keyword:
      return mutate_performance_cube.PerformanceCubeAdjustKeywordMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_performance_cube_google_ads_conversion_action:
      return mutate_performance_cube.PerformanceCubeGoogleAdsConversionActionMutator(task=task)
    elif task.task_type is models.ReportTaskType.mutate_performance_cube_global:
      return mutate_performance_cube.PerformanceCubeGlobalMutator(task=task)
    elif task.task_type is models.ReportTaskType.mutate_filter_performance_cube_channels:
      return mutate_performance_cube.FilterPerformanceCubeChannelsMutator(task=task)
    elif task.task_type is models.ReportTaskType.mutate_filter_performance_cube_mmps:
      return mutate_performance_cube.FilterPerformanceCubeMMPsMutator(task=task)
    elif task.task_type is models.ReportTaskType.mutate_filter_performance_cube_organic:
      return mutate_performance_cube.FilterPerformanceCubeOrganicMutator(task=task)
    elif task.task_type is models.ReportTaskType.mutate_performance_cube_tags:
      return mutate_performance_cube_global.TagsPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.latest_performance_cube_apple_search_ads:
      return mutate_performance_cube_global.LatestApplePerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.latest_performance_cube_facebook:
      return mutate_performance_cube_global.LatestFacebookPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.latest_performance_cube_google_ads:
      return mutate_performance_cube_global.LatestGoogleAdsPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.latest_performance_cube_snapchat:
      return mutate_performance_cube_global.LatestSnapchatPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.latest_performance_cube_tiktok:
      return mutate_performance_cube_global.LatestTikTokPerformanceCubeMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_entity_campaign:
      return mutate_entity.EntityCampaignMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_entity_adset:
      return mutate_entity.EntityAdsetMutator(task=task)
    elif task.task_type is models.ReportTaskType.materialize_entity_ad:
      return mutate_entity.EntityAdMutator(task=task)
    elif task.task_type is models.ReportTaskType.fetch_appsflyer_data_locker:
      return mutate_appsflyer.AppsFlyerDataLockerCrystallizationMutator(task=task)
    elif task.task_type is models.ReportTaskType.fetch_google_ads_ads_customer:
      return mutate_google_ads.GoogleAdsAdsCustomerMutator(task=task)
    else:
      return base.VoidReportMutator(task=task)
  elif behavior.behavior_subtype is models.ReportTaskBehaviorSubType.replace:
    return base.ReplaceReportMutator(task=task)
  elif behavior.behavior_subtype is models.ReportTaskBehaviorSubType.mutate_currency_exchange:
    return base.CurrencyConversionMutator(task=task)
  elif behavior.behavior_subtype is models.ReportTaskBehaviorSubType.mutate_latest_column_value:
    return base.LatestColumnValueMutator(task=task, specifier=behavior.behavior_subtype_info)
  elif behavior.behavior_subtype is models.ReportTaskBehaviorSubType.mutate_parse_tags:
    return mutate_tag.ParseTagsMutator(task=task, specifier=behavior.behavior_subtype_info)
  else:
    error_values = {
      'task': task,
      'behavior': behavior
    }
    raise ValueError('Mutator for behavior subtype is not supported', error_values)