import tasks
import models

from . import fetch_date_base
from . import fetch_date_currency_exchange
from . import fetch_date_appsflyer
from . import fetch_date_adjust
from . import fetch_date_performance_cube

def report_date_fetcher(task: tasks.FetchReportTask, behavior: models.ReportTaskBehavior) -> fetch_date_base.ReportDateFetcher:
  if task.task_type is models.ReportTaskType.fetch_google_ads_ad_conversion_actions:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_assets:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_assets_customer:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ads:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_assets:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_groups:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_campaigns:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_purchase_events:
    return fetch_date_appsflyer.AppsFlyerReportDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_install_events:
    return fetch_date_appsflyer.AppsFlyerInstallsDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_custom_events:
    return fetch_date_appsflyer.AppsFlyerCustomEventDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_daily:
    return fetch_date_adjust.AdjustCohortsMeasuresReportDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_weekly:
    return fetch_date_adjust.AdjustCohortsMeasuresReportDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_monthly:
    return fetch_date_adjust.AdjustCohortsMeasuresReportDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_data_locker:
    return fetch_date_appsflyer.AppsFlyerDataLockerDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_apple_search_ads:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_facebook:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_google_ads:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_snapchat:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_tiktok:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_appsflyer:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_appsflyer_keyword:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_data_locker:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_data_locker_keyword:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_data_locker_organic:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_adjust:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_adjust_keyword:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_performance_cube_google_ads_conversion_action:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.mutate_performance_cube_global:
    return fetch_date_performance_cube.PerformanceCubeGlobalDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.mutate_filter_performance_cube_channels:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.mutate_filter_performance_cube_mmps:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.mutate_filter_performance_cube_organic:
    return fetch_date_performance_cube.PerformanceCubeDateFetcherMaterializeDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.mutate_performance_cube_tags:
    return fetch_date_performance_cube.PerformanceCubeGlobalDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.latest_performance_cube_apple_search_ads:
    return fetch_date_performance_cube.PerformanceCubeGlobalDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.latest_performance_cube_facebook:
    return fetch_date_performance_cube.PerformanceCubeGlobalDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.latest_performance_cube_google_ads:
    return fetch_date_performance_cube.PerformanceCubeGlobalDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.latest_performance_cube_snapchat:
    return fetch_date_performance_cube.PerformanceCubeGlobalDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.latest_performance_cube_tiktok:
    return fetch_date_performance_cube.PerformanceCubeGlobalDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_entity_campaign:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_entity_adset:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.materialize_entity_ad:
    return fetch_date_base.CurrentDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_base_currency_exchage_rates:
    return fetch_date_currency_exchange.CurrencyExchangeReportDateFetcher(task=task)
  elif task.task_type is models.ReportTaskType.verify_performance_cube_unfiltered_apple:
    return fetch_date_base.CurrentDateFetcher(task=task)
  else:
    return fetch_date_base.BaseReportDateFetcher(task=task)