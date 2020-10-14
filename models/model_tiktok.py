import sqlalchemy as alchemy

from . import base
from typing import Optional

class TikTokReportTableModel(base.ReportTableModel):
  @property
  def date_format_string(self) -> Optional[str]:
    return '%Y-%m-%d %H:%M:%S'

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('advertiser_id', alchemy.Text),
      schema=self.schema_name
    )

class TikTokCampaignReportTableModel(TikTokReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'campaign_stat_datetime'

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_tiktok_campaigns.value

class TikTokAdGroupReportTableModel(TikTokReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'adgroup_stat_datetime'

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_tiktok_adgroups.value

class TikTokAdReportTableModel(TikTokReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'ad_stat_datetime'

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_tiktok_ads.value

# class TikTokAdGroupReportTableModel(TikTokReportTableModel):
#   @property
#   def table_name(self) -> str:
#     return base.ReportTaskType.fetch_snapchat_adsquads.value

# class TikTokAdReportTableModel(TikTokReportTableModel):
#   @property
#   def table_name(self) -> str:
#     return base.ReportTaskType.fetch_snapchat_ads.value