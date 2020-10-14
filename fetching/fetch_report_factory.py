import tasks
import models

from . import base
from . import fetch_apple
from . import fetch_google
from . import fetch_appsflyer
from . import fetch_facebook
from . import fetch_adjust
from . import fetch_snapchat
from . import fetch_tiktok
from . import fetch_currency_exchange

def report_fetcher(task: tasks.FetchReportTask, behavior: models.ReportTaskBehavior) -> base.ReportFetcher:
  if task.report_end_date < task.report_start_date:
    return base.VoidReportFetcher(task=task)

  if task.task_type is models.ReportTaskType.fetch_google_ads_ad_conversion_actions:
    return fetch_google.GoogleAdsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_conversion_actions_customer:
    return fetch_google.GoogleAdsAdConversionActionsCustomerReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_assets:
    return fetch_google.GoogleAdsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_assets_customer:
    return fetch_google.GoogleAdsAssetsCustomerReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ads:
    return fetch_google.GoogleAdsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ads_customer:
    return fetch_google.GoogleAdsAdsCustomerReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_assets:
    return fetch_google.GoogleAdsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_assets_customer:
    return fetch_google.GoogleAdsAdAssetsCustomerReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_campaigns:
    return fetch_google.GoogleAdsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_campaigns_customer:
    return fetch_google.GoogleAdsCampaignsCustomerReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_groups:
    return fetch_google.GoogleAdsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_groups_customer:
    return fetch_google.GoogleAdsAdGroupsCustomerReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_campaigns:
    return fetch_apple.AppleCampaignsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_adgroups:
    return fetch_apple.AppleAdGroupsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_keywords:
    return fetch_apple.AppleKeywordsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_creative_sets:
    return fetch_apple.AppleCreativeSetsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_searchterms:
    return fetch_apple.AppleSearchTermsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_purchase_events:
    return fetch_appsflyer.AppsFlyerPurchaseEventsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_install_events:
    return fetch_appsflyer.AppsFlyerInstallEventsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_custom_events:
    return fetch_appsflyer.AppsFlyerCustomEventsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_data_locker:
    return fetch_appsflyer.AppsFlyerDataLockerReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_data_locker_hour_part:
    return fetch_appsflyer.AppsFlyerDataLockerHourPartReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_deliverables:
    return fetch_adjust.AdjustDeliverablesReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_events:
    return fetch_adjust.AdjustEventsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_daily:
    return fetch_adjust.AdjustCohortsMeasuresReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_weekly:
    return fetch_adjust.AdjustCohortsMeasuresReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_monthly:
    return fetch_adjust.AdjustCohortsMeasuresReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_campaigns:
    return fetch_facebook.FacebookCampaignsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_adsets:
    return fetch_facebook.FacebookAdSetsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_ads:
    return fetch_facebook.FacebookAdsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_campaigns:
    return fetch_snapchat.SnapchatCampaignsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_adsquads:
    return fetch_snapchat.SnapchatAdSquadsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_ads:
    return fetch_snapchat.SnapchatAdsReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_campaigns:
    return fetch_tiktok.TikTokReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_adgroups:
    return fetch_tiktok.TikTokReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_ads:
    return fetch_tiktok.TikTokReportFetcher(task=task)
  elif task.task_type is models.ReportTaskType.fetch_base_currency_exchage_rates:
    return fetch_currency_exchange.CurrencyExchangeReportFetcher(task=task)
  else:
    return base.VoidReportFetcher(task=task)
