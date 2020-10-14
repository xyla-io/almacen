import tasks
import sqlalchemy as alchemy

from typing import Optional, Dict, TypeVar, Generic
from datetime import datetime
from subir import Uploader

T = TypeVar(tasks.FetchReportTask)
class ReportCollector(Generic[T]):
  task: T

  def __init__(self, task: T):
    self.task = task
  
  @property
  def column_type_transform_dictionary(self) -> Optional[Dict[str, any]]:
    return None

  @property
  def transform_data_frame(self) -> bool:
    return False

  def collect(self):
    self.task.sql_layer.insert_data_frame(
      data_frame=self.task.report,
      table_name=self.task.report_table_model.table_name,
      schema_name=self.task.report_table_model.schema_name,
      column_type_transform_dictionary=self.column_type_transform_dictionary,
      accept_invalid_characters=self.task.accept_invalid_characters,
      empty_as_null=self.task.empty_as_null,
      transform_data_frame=self.transform_data_frame
    )
    self.task.row_count = len(self.task.report)
    self.set_task_end_date_to_crystallized_end_date()

  def set_task_end_date_to_crystallized_end_date(self):
    # reset the task's report_end_date to the crystallized end date
    crystallized_end_date = datetime.combine(self.task.run_date - self.task.crystallization_time, datetime.min.time())
    self.task.uncrystallized_report_end_date = self.task.report_end_date
    self.task.report_end_date = min(self.task.report_end_date, crystallized_end_date)


class BaseReportCollector(ReportCollector[tasks.ReportTask]):
  pass

U = TypeVar(tasks.UpsertReportTask)
class UpsertReportCollector(Generic[U], ReportCollector[U]):
  def collect(self):
    uploader = Uploader()
    uploader.upload_data_frame(
      schema_name=self.task.report_table_model.schema_name,
      table_name=self.task.report_table_model.table_name,
      merge_column_names=self.task.merge_column_names,
      data_frame=self.task.report,
      column_type_transform_dictionary=self.column_type_transform_dictionary,
      replace=False,
      accept_invalid_characters=self.task.accept_invalid_characters,
      empty_as_null=self.task.empty_as_null,
      transform_data_frame=self.transform_data_frame
    )
    self.task.row_count = len(self.task.report)
    self.set_task_end_date_to_crystallized_end_date()

class VoidReportCollector(ReportCollector[tasks.ReportTask]):
  def collect(self):
    self.set_task_end_date_to_crystallized_end_date()