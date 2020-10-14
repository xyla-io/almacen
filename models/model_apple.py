import sqlalchemy as alchemy

from . import base
from typing import Optional

class AppleReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'date'

class AppleCampaignReportTableModel(AppleReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_apple_search_ads_campaigns.value

  def define_table(self):
    alchemy.Table(
      self.table_name, 
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('org_id', alchemy.Text),
      alchemy.Column('adamId', alchemy.Text),
      schema=self.schema_name
    )

class AppleAdGroupReportTableModel(AppleReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_apple_search_ads_adgroups.value

  def define_table(self):
    alchemy.Table(
      self.table_name, 
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('org_id', alchemy.Text),
      alchemy.Column('adamId', alchemy.Text),
      schema=self.schema_name
    )

class AppleKeywordReportTableModel(AppleReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_apple_search_ads_keywords.value

  def define_table(self):
    alchemy.Table(
      self.table_name, 
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('org_id', alchemy.Text),
      alchemy.Column('adamId', alchemy.Text),
      schema=self.schema_name
    )

class AppleCreativeSetReportTableModel(AppleReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_apple_search_ads_creative_sets.value

  def define_table(self):
    alchemy.Table(
      self.table_name, 
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('org_id', alchemy.Text),
      alchemy.Column('adamId', alchemy.Text),
      schema=self.schema_name
    )

class AppleSearchTermReportTableModel(AppleReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_apple_search_ads_searchterms.value

  def define_table(self):
    alchemy.Table(
      self.table_name, 
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('org_id', alchemy.Text),
      alchemy.Column('adamId', alchemy.Text),
      schema=self.schema_name
    )