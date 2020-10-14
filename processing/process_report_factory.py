import tasks
import models

from . import base
from . import process_google_ads
from . import process_apple
from . import process_adjust
from . import process_appsflyer
from . import process_facebook
from . import process_snapchat
from . import process_tiktok

def report_processor(task: tasks.ReportTask, behavior: models.ReportTaskBehavior) -> base.ReportProcessor:
  if task.report is None or task.report.empty:
    return base.VoidReportProcessor(task=task)
  if behavior.behavior_subtype is models.ReportTaskBehaviorSubType.edit:
    return base.EditReportProcessor(task=task)
    
  if task.task_type is models.ReportTaskType.fetch_google_ads_ad_conversion_actions_customer:
    return process_google_ads.GoogleAdsAdConversionActionCustomerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_assets_customer:
    return process_google_ads.GoogleAdsAssetsCustomerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ads_customer:
    return process_google_ads.GoogleAdsAdsCustomerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_assets_customer:
    return process_google_ads.GoogleAdsAdAssetsCustomerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_groups_customer:
    return process_google_ads.GoogleAdsAdGroupsCustomerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_campaigns_customer:
    return process_google_ads.GoogleAdsCampaignsCustomerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_campaigns:
    return process_apple.AppleReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_adgroups:
    return process_apple.AppleReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_keywords:
    return process_apple.AppleKeywordsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_creative_sets:
    return process_apple.AppleCreativeSetsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_searchterms:
    return process_apple.AppleSearchTermsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_purchase_events:
    return process_appsflyer.AppsFlyerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_install_events:
    return process_appsflyer.AppsFlyerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_custom_events:
    return process_appsflyer.AppsFlyerReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_data_locker_hour_part:
    return process_appsflyer.AppsFlyerDataLockerHourPartReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_deliverables:
    return process_adjust.AdjustDeliverablesReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_events:
    return process_adjust.AdjustEventsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_daily:
    return process_adjust.AdjustCohortsMeasuresReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_weekly:
    return process_adjust.AdjustCohortsMeasuresReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_monthly:
    return process_adjust.AdjustCohortsMeasuresReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_campaigns:
    return process_facebook.FacebookCampaignsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_adsets:
    return process_facebook.FacebookAdSetsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_ads:
    return process_facebook.FacebookAdsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_campaigns:
    return process_snapchat.SnapchatCampaignsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_adsquads:
    return process_snapchat.SnapchatAdSquadsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_ads:
    return process_snapchat.SnapchatAdsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_campaigns:
    return process_tiktok.TikTokCampaignsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_adgroups:
    return process_tiktok.TikTokAdGroupsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_ads:
    return process_tiktok.TikTokAdsReportProcessor(task=task)
  elif task.task_type is models.ReportTaskType.fetch_base_currency_exchage_rates:
    return base.CurrencyExchangeReportProcessor(task=task)
  else:
    return base.VoidReportProcessor(task=task)
