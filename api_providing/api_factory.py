import tasks
import models

from . import base
from . import api_apple
from . import api_appsflyer
from . import api_google
from . import api_adjust
from . import api_facebook
from . import api_snapchat
from . import api_tiktok
from . import api_currency_exchange

def api_provider(task: tasks.ReportTask, behavior: models.ReportTaskBehavior) -> base.APIProvider:
  if task.task_type is models.ReportTaskType.fetch_appsflyer_purchase_events:
    return api_appsflyer.AppsFlyerAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_install_events:
    return api_appsflyer.AppsFlyerAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_custom_events:
    return api_appsflyer.AppsFlyerAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_appsflyer_data_locker:
    return api_appsflyer.AppsFlyerDataLockerAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_campaigns:
    return api_apple.AppleAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_conversion_actions:
    return api_google.GoogleAdsAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_assets:
    return api_google.GoogleAdsAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ads:
    return api_google.GoogleAdsAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_assets:
    return api_google.GoogleAdsAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_campaigns:
    return api_google.GoogleAdsAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_google_ads_ad_groups:
    return api_google.GoogleAdsAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_adgroups:
    return api_apple.AppleAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_keywords:
    return api_apple.AppleAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_creative_sets:
    return api_apple.AppleAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_searchterms:
    return api_apple.AppleAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_deliverables:
    return api_adjust.AdjustAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_events:
    return api_adjust.AdjustAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_daily:
    return api_adjust.AdjustAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_weekly:
    return api_adjust.AdjustAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_adjust_cohorts_measures_monthly:
    return api_adjust.AdjustAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_campaigns:
    return api_facebook.FacebookAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_adsets:
    return api_facebook.FacebookAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_facebook_ads:
    return api_facebook.FacebookAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_campaigns:
    return api_snapchat.SnapchatAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_adsquads:
    return api_snapchat.SnapchatAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_snapchat_ads:
    return api_snapchat.SnapchatAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_campaigns:
    return api_tiktok.TikTokAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_adgroups:
    return api_tiktok.TikTokAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_tiktok_ads:
    return api_tiktok.TikTokAPIProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_base_currency_exchage_rates:
    return api_currency_exchange.CurrencyExchangeAPIProvider(task=task)
  else:
    raise ValueError('Unsupported task.task_type', task.task_type)