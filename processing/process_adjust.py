import tasks
import re
import pandas as pd

from . import base
from typing import Dict, Generic, TypeVar
from datetime import datetime, timedelta

T = TypeVar(tasks.FetchAdjustReportTask)
class AdjustReportProcessor(Generic[T], base.ReportProcessor[T]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'app_display_name': self.task.app_display_name,
      'app_token': self.task.app_token,
    }

  @property
  def null_column_defaults(self) -> Dict[str, any]:
    return {
      'event_token': '',
    }

  def process(self):
    self.drop_collected_and_partial_data(report=self.task.report)
    super().process()
    self.task.report = self.with_column_split_into_name_and_id(report=self.task.report, column='campaign')
    self.task.report = self.with_column_split_into_name_and_id(report=self.task.report, column='adgroup')
    self.task.report = self.with_column_split_into_name_and_id(report=self.task.report, column='creative')

  def drop_collected_and_partial_data(self, report: pd.DataFrame):
    date_column = self.task.report_table_model.date_column_name
    report.drop(report.index[report[date_column] < self.task.report_start_date], inplace=True)
    report.drop(report.index[report[date_column] > self.task.report_end_date], inplace=True)

  def with_column_split_into_name_and_id(self, report: pd.DataFrame, column: str) -> pd.DataFrame:
    report.reset_index(drop=True, inplace=True)
    column_parts = report[column].apply(lambda s: re.findall(r'^(.+?) *\(([^() ]+)\)$', s) if not pd.isna(s) else None).apply(lambda l: l[0] if l else (None, None))
    report = report.join(pd.DataFrame(column_parts.tolist(), columns=[f'{column}_name', f'{column}_id']))
    return report

class AdjustDeliverablesReportProcessor(AdjustReportProcessor[tasks.FetchAdjustDeliverablesReportTask]):
  pass

class AdjustEventsReportProcessor(AdjustReportProcessor[tasks.FetchAdjustEventsReportTask]):
  pass

class AdjustCohortsMeasuresReportProcessor(AdjustReportProcessor[tasks.FetchAdjustCohortsMeasuresReportTask]):
  def process(self):
    report = self.task.report
    date_column = self.task.report_table_model.date_column_name
    days_in_period = self.task.period.days
    report[date_column] = report.cohort + report.period.apply(lambda p: timedelta(days=days_in_period * p))
    super().process()
