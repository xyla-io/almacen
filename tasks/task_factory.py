import models

from . import base
from . import tasks_fetch_adjust
from . import tasks_fetch_apple
from . import tasks_fetch_google_ads
from . import tasks_fetch_appsflyer
from . import tasks_fetch_appsflyer_data_locker
from . import tasks_fetch_facebook
from . import tasks_fetch_snapchat
from . import tasks_fetch_tiktok
from . import tasks_fetch_currency_exchange
from . import tasks_mutate_performance_cube
from . import tasks_mutate_entity
from . import tasks_verify_performance_cube

from models import ReportTaskType as Type
from config import CompanyConfiguration
from typing import Dict

def report_task(task_set: CompanyConfiguration.TaskSet, report_type: Type, identifier_prefix: str) -> base.ReportTask:
  task_init_args = {
    'task_set': task_set,
    'identifier_prefix': identifier_prefix
  }
  
  if report_type is Type.fetch_google_ads_ad_conversion_actions:
    return tasks_fetch_google_ads.FetchGoogleAdsAdConversionActionsReportTask(**task_init_args)
  elif report_type is Type.fetch_google_ads_assets:
    return tasks_fetch_google_ads.FetchGoogleAdsAssetsReportTask(**task_init_args)
  elif report_type is Type.fetch_google_ads_ads:
    return tasks_fetch_google_ads.FetchGoogleAdsAdsReportTask(**task_init_args)
  elif report_type is Type.fetch_google_ads_ad_assets:
    return tasks_fetch_google_ads.FetchGoogleAdsAdAssetsReportTask(**task_init_args)
  elif report_type is Type.fetch_google_ads_ad_groups:
    return tasks_fetch_google_ads.FetchGoogleAdsAdGroupsReportTask(**task_init_args)
  elif report_type is Type.fetch_google_ads_campaigns:
    return tasks_fetch_google_ads.FetchGoogleAdsCampaignsReportTask(**task_init_args)
  if report_type is Type.fetch_apple_search_ads_campaigns:
    return tasks_fetch_apple.FetchAppleCampaignsReportTask(**task_init_args)
  elif report_type is Type.fetch_apple_search_ads_adgroups:
    return tasks_fetch_apple.FetchAppleAdGroupsReportTask(**task_init_args)
  elif report_type is Type.fetch_apple_search_ads_keywords:
    return tasks_fetch_apple.FetchAppleKeywordsReportTask(**task_init_args)
  elif report_type is Type.fetch_apple_search_ads_creative_sets:
    return tasks_fetch_apple.FetchAppleCreativeSetsReportTask(**task_init_args)
  elif report_type is Type.fetch_apple_search_ads_searchterms:
    return tasks_fetch_apple.FetchAppleSearchTermsReportTask(**task_init_args)
  elif report_type is Type.fetch_appsflyer_purchase_events:
    return tasks_fetch_appsflyer.FetchAppsFlyerPurchaseEventsReportTask(**task_init_args)
  elif report_type is Type.fetch_appsflyer_install_events:
    return tasks_fetch_appsflyer.FetchAppsFlyerInstallEventsReportTask(**task_init_args)
  elif report_type is Type.fetch_appsflyer_custom_events:
    return tasks_fetch_appsflyer.FetchAppsFlyerCustomEventsReportTask(**task_init_args)
  elif report_type is Type.fetch_appsflyer_data_locker:
    return tasks_fetch_appsflyer_data_locker.FetchAppsFlyerDataLockerReportTask(**task_init_args)
  elif report_type is Type.fetch_facebook_campaigns:
    return tasks_fetch_facebook.FetchFacebookCampaignsReportTask(**task_init_args)
  elif report_type is Type.fetch_facebook_adsets:
    return tasks_fetch_facebook.FetchFacebookAdSetsReportTask(**task_init_args)
  elif report_type is Type.fetch_facebook_ads:
    return tasks_fetch_facebook.FetchFacebookAdsReportTask(**task_init_args)
  elif report_type is Type.fetch_snapchat_campaigns:
    return tasks_fetch_snapchat.FetchSnapchatCampaignsReportTask(**task_init_args)
  elif report_type is Type.fetch_snapchat_adsquads:
    return tasks_fetch_snapchat.FetchSnapchatAdSquadsReportTask(**task_init_args)
  elif report_type is Type.fetch_snapchat_ads:
    return tasks_fetch_snapchat.FetchSnapchatAdsReportTask(**task_init_args)
  elif report_type is Type.fetch_tiktok_campaigns:
    return tasks_fetch_tiktok.FetchTikTokCampaignsReportTask(**task_init_args)
  elif report_type is Type.fetch_tiktok_adgroups:
    return tasks_fetch_tiktok.FetchTikTokAdGroupsReportTask(**task_init_args)
  elif report_type is Type.fetch_tiktok_ads:
    return tasks_fetch_tiktok.FetchTikTokAdsReportTask(**task_init_args)
  elif report_type is Type.fetch_adjust_deliverables:
    return tasks_fetch_adjust.FetchAdjustDeliverablesReportTask(**task_init_args)
  elif report_type is Type.fetch_adjust_events:
    return tasks_fetch_adjust.FetchAdjustEventsReportTask(**task_init_args)
  elif report_type is Type.fetch_adjust_cohorts_measures_daily:
    return tasks_fetch_adjust.FetchAdjustCohortsMeasuresDailyReportTask(**task_init_args)
  elif report_type is Type.fetch_adjust_cohorts_measures_weekly:
    return tasks_fetch_adjust.FetchAdjustCohortsMeasuresWeeklyReportTask(**task_init_args)
  elif report_type is Type.fetch_adjust_cohorts_measures_monthly:
    return tasks_fetch_adjust.FetchAdjustCohortsMeasuresMonthlyReportTask(**task_init_args)
  elif report_type is Type.fetch_currency_exchange_rates:
    return tasks_fetch_currency_exchange.FetchCurrencyExchangeReportTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_apple_search_ads:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeAppleTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_facebook:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeFacebookTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_google_ads:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeGoogleAdsTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_snapchat:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeSnapchatTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_tiktok:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeTikTokTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_appsflyer:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeAppsFlyerTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_appsflyer_keyword:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeAppsFlyerKeywordTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_data_locker:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeDataLockerTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_data_locker_keyword:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeDataLockerKeywordTask(**task_init_args)
  elif report_type is Type.materialize_performance_cube_data_locker_organic:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeDataLockerOrganicTask(**task_init_args)
  elif report_type is models.ReportTaskType.materialize_performance_cube_adjust:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeAdjustTask(**task_init_args)
  elif report_type is models.ReportTaskType.materialize_performance_cube_adjust_keyword:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeAdjustKeywordTask(**task_init_args)
  elif report_type is models.ReportTaskType.materialize_performance_cube_google_ads_conversion_action:
    return tasks_mutate_performance_cube.MaterializePerformanceCubeGoogleAdsConversionActionTask(**task_init_args)
  elif report_type is models.ReportTaskType.materialize_entity_campaign:
    return tasks_mutate_entity.MaterializeEntityCampaignTask(**task_init_args)
  elif report_type is models.ReportTaskType.materialize_entity_adset:
    return tasks_mutate_entity.MaterializeEntityAdsetTask(**task_init_args)
  elif report_type is models.ReportTaskType.materialize_entity_ad:
    return tasks_mutate_entity.MaterializeEntityAdTask(**task_init_args)
  elif report_type is Type.mutate_performance_cube_global:
    return tasks_mutate_performance_cube.MutatePerformanceCubeGlobalTask(**task_init_args)
  elif report_type is Type.mutate_filter_performance_cube_channels:
    return tasks_mutate_performance_cube.FilterPerformanceCubeChannelsTask(**task_init_args)
  elif report_type is Type.mutate_filter_performance_cube_mmps:
    return tasks_mutate_performance_cube.FilterPerformanceCubeMMPsTask(**task_init_args)
  elif report_type is Type.mutate_filter_performance_cube_organic:
    return tasks_mutate_performance_cube.FilterPerformanceCubeOrganicTask(**task_init_args)
  elif report_type is Type.mutate_performance_cube_tags:
    return tasks_mutate_performance_cube.MutatePerformanceCubeTagsTask(**task_init_args)
  elif report_type is Type.latest_performance_cube_apple_search_ads:
    return tasks_mutate_performance_cube.MutatePerformanceCubeLatestAppleTask(**task_init_args)
  elif report_type is Type.latest_performance_cube_facebook:
    return tasks_mutate_performance_cube.MutatePerformanceCubeLatestFacebookTask(**task_init_args)
  elif report_type is Type.latest_performance_cube_google_ads:
    return tasks_mutate_performance_cube.MutatePerformanceCubeLatestGoogleAdsTask(**task_init_args)
  elif report_type is Type.latest_performance_cube_snapchat:
    return tasks_mutate_performance_cube.MutatePerformanceCubeLatestSnapchatTask(**task_init_args)
  elif report_type is Type.latest_performance_cube_tiktok:
    return tasks_mutate_performance_cube.MutatePerformanceCubeLatestTikTokTask(**task_init_args)
  elif report_type is Type.verify_performance_cube_unfiltered_apple:
    return tasks_verify_performance_cube.VerifyPerformanceCubeUnfilteredAppleTask(**task_init_args)
  else:
    raise ValueError('Unsupported report task.task_type', report_type)