import models
import pandas as pd

from . import base
from config import CompanyConfiguration
from salem import AppsFlyerDataLockerAPI, AppsFlyerDataLockerReporter
from typing import Optional, Dict, List
from datetime import datetime, timedelta
from abc import abstractmethod

class FetchAppsFlyerDataLockerReportBaseTask(base.FetchReportTask):
  @property
  def crystallization_time(self) -> timedelta:
    return timedelta(seconds=0)

  @property
  def collection_threshold(self) -> Optional[int]:
    return None

  @property
  @abstractmethod
  def report_parts_context(self) -> base.FetchReportTask:
    pass

class FetchAppsFlyerDataLockerHourPartReportTask(base.FetchReportTask):
  api: AppsFlyerDataLockerAPI
  report_parts_context: base.FetchReportTask
  hour_part_config: Dict[str, any]

  def __init__(self, task_set: CompanyConfiguration.TaskSet, identifier_prefix: str, api: AppsFlyerDataLockerAPI, report_parts_context: base.FetchReportTask, hour_part_config: Dict[str, any]):
    super().__init__(
      task_set=task_set,
      identifier_prefix=identifier_prefix
    )
    self.api = api
    self.report_parts_context = report_parts_context
    self.hour_part_config = hour_part_config
    self.run_date = self.report_parts_context.run_date
    self.report_start_date = self.date
    self.report_end_date = self.date

  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_appsflyer_data_locker_hour_part

  @property
  def debug_description(self) -> str:
    return '{}: ({}) — {}'.format(
      self.company_display_name,
      self.task_type.value,
      '_'.join([self.data_locker_report_type, self.date.strftime('%Y-%m-%d'), str(self.hour_value), str(self.part)])
    )

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppsFlyerDataLockerReportTableModel(schema_name=self.report_table_schema)

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.before
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_report),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.process),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.process,
        behavior_subtype=models.ReportTaskBehaviorSubType.edit
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.collect),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]

  @property
  def collection_threshold(self) -> int:
    return 100000

  @property
  def format_integer_id_columns(self) -> bool:
    return self.task_set.config['format_integer_id_columns'] if 'format_integer_id_columns' in self.task_set.config else False

  @property
  def data_locker_report_type(self) -> str:
    return self.task_set.config['data_locker_report_type']
  
  @property
  def date(self) -> datetime:
    return self.hour_part_config['hour_part']['date']

  @property
  def hour_value(self) -> int:
    return self.hour_part_config['hour_part']['hour_value']

  @property
  def part(self) -> int:
    return self.hour_part_config['hour_part']['part']

  @property
  def last_part(self) -> int:
    return self.hour_part_config['hour_part']['last_part']

class FetchAppsFlyerDataLockerReportTask(FetchAppsFlyerDataLockerReportBaseTask):
  api: Optional[AppsFlyerDataLockerAPI] = None
  report_start_hour: Optional[int] = None
  report_start_part: Optional[int] = None
  hour_part_subtask_configurations: Optional[List[Dict[str, any]]] = None

  @property
  def debug_description(self) -> str:
    return '{}: ({}) — {}'.format(
      self.company_display_name,
      self.task_type.value,
      self.data_locker_report_type
    )

  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_appsflyer_data_locker

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppsFlyerDataLockerReportTableModel(schema_name=self.report_table_schema)
  
  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'data_locker_report_type': self.data_locker_report_type,
    }

  @property
  def report_parts_context(self) -> base.FetchReportTask:
    return self

  @property
  def data_locker_report_type(self) -> str:
    return self.task_set.config['data_locker_report_type']

  @property
  def hourly_data_path(self) -> str:
    return self.task_set.config['hourly_data_path']

  @property
  def bucket_name(self) -> str:
    return self.task_set.config['bucket_name'] if 'bucket_name' in self.task_set.config else 'af-ext-reports'
  
  @property
  def bucket_region(self) -> str:
    return self.task_set.config['bucket_region'] if 'bucket_region' in self.task_set.config else 'eu-west-1'

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_date),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.provide_credentials),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.provide_api),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.before
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_report),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.run_subtasks),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.collect),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.mutate),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]

  def generate_subtasks(self) -> List[base.ReportTask]:
    if self.hour_part_subtask_configurations is None:
      return []
    self.report = pd.DataFrame()
    return [
      FetchAppsFlyerDataLockerHourPartReportTask(
        task_set=self.task_set,
        identifier_prefix='{}.{}'.format(self.identifier, '_'.join([c['date'].strftime('%Y-%m-%d'), str(c['hour_value']), str(c['part'])])),
        api=self.api,
        report_parts_context=self,
        hour_part_config={'hour_part': c}
      )
      for c in self.hour_part_subtask_configurations
    ]
