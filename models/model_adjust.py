import sqlalchemy as alchemy

from . import base
from typing import Optional

class AdjustReportTableModel(base.ReportTableModel):
  @property
  def date_format_string(self) -> Optional[str]:
    return None

class AdjustDeliverablesTableModel(AdjustReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_adjust_deliverables.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('app_token', alchemy.Text),
      schema=self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'date'

class AdjustEventsTableModel(AdjustReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_adjust_events.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('app_token', alchemy.Text),
      schema=self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'date'

class AdjustCohortsMeasuresDailyReportTableModel(AdjustReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_adjust_cohorts_measures_daily.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('app_token', alchemy.Text),
      schema = self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'day'

class AdjustCohortsMeasuresWeeklyReportTableModel(AdjustReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_adjust_cohorts_measures_weekly.value

  def define_table(self):
     alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('app_token', alchemy.Text),
      schema=self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'week'

class AdjustCohortsMeasuresMonthlyReportTableModel(AdjustReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_adjust_cohorts_measures_monthly.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('app_token', alchemy.Text),
      schema=self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'month'