import models

from . import base
from typing import List, Dict, Optional
from garfield import AdjustAPI, AdjustReportPeriod
from datetime import datetime, timedelta
from abc import abstractproperty

class FetchAdjustReportTask(base.FetchReportTask):
  api: Optional[AdjustAPI]
  fetch_date: Optional[datetime]

  @property
  def debug_description(self) -> str:
    return '{}: ({}) — {}'.format(
      self.company_display_name,
      self.app_token,
      self.task_type.value
    )

  @property
  def app_token(self) -> str:
    return self.task_set.config['app_token']

  @property
  def app_display_name(self) -> str:
    assert len(self.task_set.products) == 1
    return self.task_set.products[0].display_name

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'app_token': self.app_token,
    }

  @property
  def crystallization_time(self) -> timedelta:
    return timedelta(days=2, hours=6)

class FetchAdjustDeliverablesReportTask(FetchAdjustReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_adjust_deliverables

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AdjustDeliverablesTableModel(schema_name=self.report_table_schema)

class FetchAdjustEventsReportTask(FetchAdjustReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_adjust_events

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AdjustEventsTableModel(schema_name=self.report_table_schema)

class FetchAdjustCohortsMeasuresReportTask(FetchAdjustReportTask):
  @abstractproperty
  def period(self) -> AdjustReportPeriod:
    pass

  @property
  def first_cohort_date(self) -> datetime:
    return datetime.strptime(self.task_set.config['first_cohort_date'], '%Y-%m-%d')

  @property
  def cohort_days(self) -> int:
    return 1

  @property
  def crystallization_time(self) -> timedelta:
    return timedelta(days=self.period.days + self.cohort_days + 1, hours=6)

class FetchAdjustCohortsMeasuresDailyReportTask(FetchAdjustCohortsMeasuresReportTask):
  @property
  def period(self) -> AdjustReportPeriod:
    return AdjustReportPeriod.day

  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_adjust_cohorts_measures_daily

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AdjustCohortsMeasuresDailyReportTableModel(schema_name=self.report_table_schema)

class FetchAdjustCohortsMeasuresWeeklyReportTask(FetchAdjustCohortsMeasuresReportTask):
  @property
  def period(self) -> AdjustReportPeriod:
    return AdjustReportPeriod.week

  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_adjust_cohorts_measures_weekly

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AdjustCohortsMeasuresWeeklyReportTableModel(schema_name=self.report_table_schema)

class FetchAdjustCohortsMeasuresMonthlyReportTask(FetchAdjustCohortsMeasuresReportTask):
  @property
  def period(self) -> AdjustReportPeriod:
    return AdjustReportPeriod.month

  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_adjust_cohorts_measures_monthly

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AdjustCohortsMeasuresMonthlyReportTableModel(schema_name=self.report_table_schema)
