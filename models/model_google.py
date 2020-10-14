import sqlalchemy as alchemy

from . import base
from typing import Optional

class GoogleReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'Day'

class GoogleAdsAdConversionActionReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'segments_date'

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_google_ads_ad_conversion_actions.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('customer_id', alchemy.Text),
      schema=self.schema_name
    )

class GoogleAdsAssetReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'fetch_date'

  @property
  def date_format_string(self) -> Optional[str]:
    return None

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_google_ads_assets.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('customer_id', alchemy.Text),
      schema=self.schema_name
    )

class GoogleAdsAdReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'segments_date'

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_google_ads_ads.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('customer_id', alchemy.Text),
      alchemy.Column('cost', alchemy.Float),
      schema=self.schema_name
    )

class GoogleAdsAdAssetReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'segments_date'

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_google_ads_ad_assets.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('customer_id', alchemy.Text),
      alchemy.Column('cost', alchemy.Float),
      schema=self.schema_name
    )

class GoogleAdsAdGroupReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'segments_date'

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_google_ads_ad_groups.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('customer_id', alchemy.Text),
      alchemy.Column('cost', alchemy.Float),
      schema=self.schema_name
    )

class GoogleAdsCampaignReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'segments_date'

  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_google_ads_campaigns.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('customer_id', alchemy.Text),
      alchemy.Column('cost', alchemy.Float),
      schema=self.schema_name
    )