import sqlalchemy as alchemy

from . import base
from typing import Optional

class AppsFlyerReportTableModel(base.ReportTableModel):
  @property
  def date_column_name(self) -> str:
    return 'Event Time'

  @property
  def date_format_string(self) -> Optional[str]:
    return '%Y-%m-%d %H:%M:%S'

class AppsFlyerPurchaseEventsReportTableModel(AppsFlyerReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_appsflyer_purchase_events.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('app_id', alchemy.Text),
      schema=self.schema_name
    )

class AppsFlyerInstallEventsReportTableModel(AppsFlyerReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_appsflyer_install_events.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('app_id', alchemy.Text),
      schema=self.schema_name
    )

class AppsFlyerCustomEventsReportTableModel(AppsFlyerReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_appsflyer_custom_events.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('app_id', alchemy.Text),
      alchemy.Column('event name', alchemy.Text),
      schema=self.schema_name
    )

class AppsFlyerDataLockerReportTableModel(AppsFlyerReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_appsflyer_data_locker.value

  @property
  def date_column_name(self) -> str:
    return 'data_locker_date'

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('data_locker_report_type', alchemy.Text),
      alchemy.Column('data_locker_hour', alchemy.Integer),
      alchemy.Column('data_locker_end_part', alchemy.Integer),
      alchemy.Column('effective_date', alchemy.Date),
      schema=self.schema_name
    )