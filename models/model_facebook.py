import sqlalchemy as alchemy

from . import base
from typing import Optional

class FacebookReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'date_start'

class FacebookCampaignReportTableModel(FacebookReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_facebook_campaigns.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('account_id', alchemy.Text),
      schema=self.schema_name
    )

class FacebookAdSetReportTableModel(FacebookReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_facebook_adsets.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('account_id', alchemy.Text),
      schema=self.schema_name
    )

class FacebookAdReportTableModel(FacebookReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_facebook_ads.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('account_id', alchemy.Text),
      schema=self.schema_name
    )
