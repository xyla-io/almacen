from data_layer import SQLQuery
from .mutate_query_performance_cube import PerformanceCubeEntity, PerformanceCubeSelectQuery, PerformanceCubeChannelSelectQuery, PerformanceCubeUpdateQuery
from typing import List, Dict, OrderedDict as OrderedDictType
from collections import OrderedDict

# ---------------------------------------------------------------------------
# Apple
# ---------------------------------------------------------------------------
class ApplePerformanceCubeQuery(PerformanceCubeChannelSelectQuery):
  @property
  def channel_name(self) -> str:
    return 'Apple'

class AppleCampaignPerformanceCubeQuery(ApplePerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_apple_search_ads_campaign'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'campaign_status'
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.campaign

class AppleAdGroupPerformanceCubeQuery(ApplePerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_apple_search_ads_adgroup'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'adset_status'
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.adset

class AppleCreativeSetPerformanceCubeQuery(ApplePerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_apple_search_ads_creative_set'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'ad_status'
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.ad

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {
      **super().source_column_expressions,
      'adset_name': SQLQuery('null'),
    }

class AppleKeywordPerformanceCubeQuery(ApplePerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_apple_search_ads_keyword'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'ad_status'
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.ad

# ---------------------------------------------------------------------------
# Facebook
# ---------------------------------------------------------------------------
class FacebookPerformanceCubeQuery(PerformanceCubeChannelSelectQuery):
  @property
  def channel_name(self) -> str:
    return 'Facebook'

class FacebookAdPerformanceCubeQuery(FacebookPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_facebook_ad'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'creative_id',
      'creative_name',
      'creative_title',
      'creative_body',
      'creative_image_url',
      'creative_thumbnail_url',
      'relevance_score',
      'reach',
      'frequency',
      'ad_status',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.ad

class FacebookAdSetPerformanceCubeQuery(FacebookPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_facebook_adset'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'frequency',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.adset

class FacebookCampaignPerformanceCubeQuery(FacebookPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_facebook_campaign'
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.campaign

# ---------------------------------------------------------------------------
# Google
# ---------------------------------------------------------------------------
class GoogleAdsPerformanceCubeQuery(PerformanceCubeChannelSelectQuery):
  @property
  def channel_name(self) -> str:
    return 'Google'

class GoogleAdsAdPerformanceCubeQuery(GoogleAdsPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_google_ads_ad'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'adset_status',
      'ad_status',
      'ad_type',
      'creative_image_url',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.ad

class GoogleAdsAdGroupPerformanceCubeQuery(GoogleAdsPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_google_ads_ad_group'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'adset_status',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.adset

class GoogleAdsCampaignPerformanceCubeQuery(GoogleAdsPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_google_ads_campaign'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'campaign_status',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.campaign

# ---------------------------------------------------------------------------
# Snapchat
# ---------------------------------------------------------------------------
class SnapchatPerformanceCubeQuery(PerformanceCubeChannelSelectQuery):
  @property
  def channel_name(self) -> str:
    return 'Snapchat'

class SnapchatAdPerformanceCubeQuery(SnapchatPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_snapchat_ad'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'reach',
      'frequency',
      'campaign_objective',
      'ad_type',
      'ad_status',
      'creative_id',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.ad

class SnapchatAdSquadPerformanceCubeQuery(SnapchatPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_snapchat_adsquad'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'reach',
      'frequency',
      'adset_status',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.adset

class SnapchatCampaignPerformanceCubeQuery(SnapchatPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_snapchat_campaign'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'campaign_status',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.campaign

# ---------------------------------------------------------------------------
# TikTok
# ---------------------------------------------------------------------------
class TikTokPerformanceCubeQuery(PerformanceCubeChannelSelectQuery):
  @property
  def channel_name(self) -> str:
    return 'TikTok'

class TikTokAdPerformanceCubeQuery(TikTokPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_tiktok_ad'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'ad_type',
      'ad_status',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.ad

class TikTokAdGroupPerformanceCubeQuery(TikTokPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_tiktok_adgroup'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'adset_status',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.adset

class TikTokCampaignPerformanceCubeQuery(TikTokPerformanceCubeQuery):
  @property
  def source_name(self) -> str:
    return 'cube_tiktok_campaign'

  @property
  def source_columns(self) -> List[str]:
    return [
      *super().source_columns,
      'campaign_status',
      'campaign_objective',
    ]
  
  @property
  def entity_granularity(self) -> PerformanceCubeEntity:
    return PerformanceCubeEntity.campaign