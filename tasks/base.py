import models
import pandas as pd

from . import base
from enum import Enum
from abc import abstractmethod, abstractproperty
from datetime import datetime, timedelta
from typing import Optional, Dict, List, TypeVar
from config import CompanyConfiguration
from data_layer import SQLLayer, SQLQuery

class ReportTask:
  identifier: str
  task_set: CompanyConfiguration.TaskSet
  run_date: Optional[datetime]
  report_start_date: Optional[datetime]
  report_end_date: Optional[datetime]
  report: Optional[pd.DataFrame]
  sql_layer: Optional[SQLLayer]
  last_run_history: Optional[models.TaskHistory]
  behaviors: List[models.ReportTaskBehavior]
  subtasks: List[TypeVar('ReportTask')]
  retry: Optional[str]
  row_count: Optional[int]=None
  
  def __init__(self, task_set: CompanyConfiguration.TaskSet, identifier_prefix: str):
    self.task_set = task_set
    self.last_run_history = None
    self.identifier = self.generate_identifier(prefix=identifier_prefix)
    self.behaviors = self.generate_behaviors()
    self.sql_layer = models.SQL.Layer()
    self.run_date = None
    self.report_start_date = None
    self.report_end_date = None
    self.report = None
    self.retry = None
    self.subtasks = self.generate_subtasks()

  @abstractproperty
  def task_type(self) -> models.ReportTaskType:
    pass

  @abstractproperty
  def report_table_model(self) -> models.ReportTableModel:
    pass

  @property
  def report_table_schema(self) -> Optional[str]:
    return self.task_set.company_metadata.schema

  @property
  def schema_prefix(self) -> str:
    return f'{self.report_table_schema}.' if self.report_table_schema is not None else ''

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {}

  @property
  def task_negative_identifier_columns(self) -> Dict[str, any]:
    return {}

  @property
  def report_table_exists(self) -> bool:
    self.sql_layer.connect()
    table_exists = self.report_table_model.table_exists(sql_layer=self.sql_layer)
    self.sql_layer.disconnect()
    return table_exists

  @property
  def company_display_name(self) -> str:
    return self.task_set.company_metadata.display_name

  @property
  def verifications(self) -> Dict[str, Dict[str, any]]:
    return self.task_set.config['verifications'] if 'verifications' in self.task_set.config else {} 

  @abstractproperty
  def debug_description(self) -> str:
    pass

  @abstractmethod
  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    pass
  
  def generate_subtasks(self) -> List[TypeVar('ReportTask')]:
    return []

  def generate_identifier(self, prefix: str) -> str:
    return '{}.{}'.format(prefix, self.task_type.value)

  def filtered_alchemy_query_by_identifier_columns(self, query: any):
    for (column_name, comparison_value) in self.task_identifier_columns.items():
      column = self.report_table_model.table.columns[column_name]
      if type(comparison_value) is list:
        query = query.filter(column.in_(comparison_value))
      else:
        query = query.filter(column == comparison_value)

    for (column_name, comparison_value) in self.task_negative_identifier_columns.items():
      column = self.report_table_model.table.columns[column_name]
      if type(comparison_value) is list:
        query = query.filter(~column.in_(comparison_value))
      else:
        query = query.filter(column != comparison_value)
    
    return query

  def append_identifier_column_conditions_to_query(self, query: SQLQuery):
    conditions = []
    substitution_parameters = []
    for (column_name, comparison_value) in self.task_identifier_columns.items():
      column = self.report_table_model.table.columns[column_name]
      if type(comparison_value) is list:
        conditions.append(f'{self.report_table_model.full_table_name}."{column.name}" IN {SQLQuery.format_array(comparison_value)}')
        substitution_parameters += comparison_value
      elif comparison_value is None:
        conditions.append(f'{self.report_table_model.full_table_name}."{column.name}" IS NULL')
      else:
        conditions.append(f'{self.report_table_model.full_table_name}."{column.name}" = %s')
        substitution_parameters.append(comparison_value)

    for (column_name, comparison_value) in self.task_negative_identifier_columns.items():
      column = self.report_table_model.table.columns[column_name]
      if type(comparison_value) is list:
        conditions.append(f'{self.report_table_model.full_table_name}."{column.name}" NOT IN {SQLQuery.format_array(comparison_value)}')
        substitution_parameters += comparison_value
      elif comparison_value is None:
        conditions.append(f'{self.report_table_model.full_table_name}."{column.name}" IS NOT NULL')
      else:
        conditions.append(f'{self.report_table_model.full_table_name}."{column.name}" != %s')
        substitution_parameters.append(comparison_value)

    query.query += ' AND '.join(conditions)
    query.substitution_parameters += tuple(substitution_parameters)


class FetchReportTask(ReportTask):
  api_credentials: Optional[Dict[str, any]] = None
  uncrystallized_report_end_date: Optional[datetime] = None

  @property
  def task_type(self) -> models.ReportTaskType:
    raise NotImplementedError()

  @property
  def api_credentials_key(self) -> str:
    return self.task_set.credentials_key

  @property
  def default_fetch_columns(self) -> List[str]:
    raise NotImplementedError()

  @property
  def fetch_columns(self) -> List[str]:
    return self.task_set.config['columns'] if 'columns' in self.task_set.config else self.default_fetch_columns

  @property
  def crystallization_time(self) -> timedelta:
    return timedelta(days=3)

  @property
  def edits(self) -> List[Dict[str, any]]:
    edits = self.task_set.config['edits'] if 'edits' in self.task_set.config else []
    edits = list(map(lambda e: {'case_sensitive': False, **e}, edits))
    return list(filter(lambda e: 'task_types' not in e or not e['task_types'] or self.task_type.value in e['task_types'], edits))
  
  @property
  def accept_invalid_characters(self) -> bool:
    return self.task_set.config['accept_invalid_characters'] if 'accept_invalid_characters' in self.task_set.config else False

  @property
  def empty_as_null(self) -> bool:
    return self.task_set.config['empty_as_null'] if 'empty_as_null' in self.task_set.config else False

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
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.process),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.process,
        behavior_subtype=models.ReportTaskBehaviorSubType.edit
      ),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate, 
        behavior_subtype=models.ReportTaskBehaviorSubType.replace
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.collect),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]

class MutateReportTask(ReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    raise NotImplementedError()

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_date),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.before
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.mutate),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]

class CombinedReportTask(ReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    raise NotImplementedError()

  @property
  def report_table_model(self) -> models.ReportTableModel:
    raise NotImplementedError()

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.run_subtasks),
    ]

class UpsertReportTask(ReportTask):
  @property
  def merge_column_names(self) -> List[str]:
    return []

class VerifyReportTask(ReportTask):
  @property
  def debug_description(self) -> str:
    return f'{self.company_display_name} {self.task_type.value}'

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_date),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.verify),
    ]
