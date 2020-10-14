import sqlalchemy as alchemy

from . import base
from typing import Optional

class SnapchatReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'start_time'
  
  @property
  def date_format_string(self) -> Optional[str]:
    return '%Y-%m-%dT%H:%M:%S.%f%z'

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('ad_account_id', alchemy.Text),
      schema=self.schema_name
    )

class SnapchatCampaignReportTableModel(SnapchatReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_snapchat_campaigns.value

class SnapchatAdSquadReportTableModel(SnapchatReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_snapchat_adsquads.value

class SnapchatAdReportTableModel(SnapchatReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_snapchat_ads.value