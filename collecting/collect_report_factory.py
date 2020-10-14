import tasks
import models

from . import base
from . import collect_apple
from . import collect_appsflyer
from . import collect_google_ads

def report_collector(task: tasks.FetchReportTask, behavior: models.ReportTaskBehavior) -> base.ReportCollector:
  if task.report is None or task.report.empty:
    return base.VoidReportCollector(task=task)

  if task.task_type is models.ReportTaskType.fetch_apple_search_ads_keywords:
    return collect_apple.AppleKeywordsReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_purchase_events:
    return collect_appsflyer.AppsFlyerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_install_events:
    return collect_appsflyer.AppsFlyerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_custom_events:
    return collect_appsflyer.AppsFlyerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_data_locker:
    return collect_appsflyer.AppsFlyerDataLockerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_data_locker_hour_part:
    return collect_appsflyer.AppsFlyerDataLockerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_assets_customer:
    return collect_google_ads.GoogleAdsAssetsCustomerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ads_customer:
    return collect_google_ads.GoogleAdsAdsCustomerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_assets_customer:
    return collect_google_ads.GoogleAdsAdAssetsCustomerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_groups_customer:
    return collect_google_ads.GoogleAdsAdGroupsCustomerReportCollector(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_campaigns_customer:
    return collect_google_ads.GoogleAdsCampaignsCustomerReportCollector(task=task)
  else:
    return base.BaseReportCollector(task=task)