from __future__ import annotations

import re
import tasks
import pandas as pd

from error import VerificationError
from fabrica import Verifier
from data_layer import SQLQuery
from io import StringIO
from abc import abstractmethod
from typing import TypeVar, Generic, Optional, Dict, List

class Verification:
  """A verification of one comparison between two data sets that should match."""

  class VerificationResult:
    """A descriptive result of comparing two data sets"""
    identifier: str
    description: str
    success: bool
    required: bool

    def __init__(self, identifier: str, success: bool, required: bool, description: str):
      self.identifier = identifier
      self.success = success
      self.required = required
      self.description = description

  identifier: str
  task_pattern: Optional[re.Pattern]
  required: bool
  verify: Dict[str, any]
  before: bool
  after: bool

  def __init__(self, identifier: str, configuration: Dict[str, any]):
    self.identifier = identifier
    self.task_pattern = re.compile(configuration['task_pattern']) if 'task_pattern' in configuration else None
    self.required = configuration['required'] if 'required' in configuration else True
    self.verify = configuration['verify']
    self.before = configuration['before'] if 'before' in configuration else False
    self.after = configuration['after'] if 'after' in configuration else False

  def result_description(self, result: Verifier.Verification) -> str:
    return f'''files: {" ".join([
      result.json_path_a,
      result.json_path_b,
      result.csv_path_a,
      result.csv_path_b
    ])}'''

  def matches_task_type(self, task_type: str) -> bool:
    return self.task_pattern is None or self.task_pattern.match(task_type) is not None

  def verify_task(self, task: tasks.ReportTask) -> Verification.VerificationResult:
    verifier = Verifier(database=task.sql_layer.connection_options.database)
    result = verifier.verify(**self.verify)
    return Verification.VerificationResult(
      identifier=self.identifier,
      success=result.success,
      required=self.required,
      description=self.result_description(result=result)
    )

T = TypeVar(tasks.ReportTask)
"""Generic ReportTask"""

class ReportVerifier(Generic[T]):
  """An abstract base verifier for performing a set of verifications."""
  task: T

  def __init__(self, task: 'T'):
    self.task = task

  @abstractmethod
  def verify(self):
    pass

class BaseReportVerifier(ReportVerifier):
  """A concrete base verifier that performs all verifications matching a task type."""
  @property
  def verifications(self) -> List[Verification]:
    return list(filter(lambda v: v.matches_task_type(self.task.task_type.value), [
      Verification(
        identifier=k,
        configuration=v
      )
      for k, v in sorted(self.task.verifications.items(), key=lambda t: t[0])
    ]))

  def verify(self):
    verifications = self.verifications
    if not verifications:
      return
    results = [
      v.verify_task(task=self.task)
      for v in verifications
    ]
    error_descriptions = [
      f'{r.identifier} ({r.description})' for r in results if not r.success and r.required
    ]
    if error_descriptions:
      raise VerificationError(verifications=error_descriptions)
    newline = '\n'
    return f'{len(list(filter(lambda r: r.success, results)))} / {len(results)} verification{"s" if len(results) != 1 else ""} passed:\n{newline.join("Verification " + ("succeeded" if r.success else "FAILED") + f" ——> {r.identifier}" + (" (required)" if r.required else "") + (f" {r.description}" if r.description else "") for r in results)}'

class BeforeReportVerifier(BaseReportVerifier):
  """A concrete verifier that performs all verifications that should occur before main task execution."""
  @property
  def verifications(self) -> List[Verification]:
    return list(filter(lambda v: v.before, super().verifications))

class AfterReportVerifier(BaseReportVerifier):
  """A concrete verifier that performs all verifications that should occur after main task execution."""
  @property
  def verifications(self) -> List[Verification]:
    print('verifications')
    return [
      *([RowCountVerification(
        task=self.task,
        required=True,
        allow_empty=True
      )] if self.task.run_date is not None and self.task.row_count is not None else []),
      *filter(lambda v: v.after, super().verifications),
    ]

class VoidReportVerifier(ReportVerifier):
  """A concrete verifier that performs no verifications at all."""
  def verify(self):
    pass

class RowCountVerification(Verification):
  """A verification that the task's row count matches the number of rows in its table with a fetch_date value of the task's run_date."""
  task: tasks.ReportTask
  allow_empty: bool

  def __init__(self, task: tasks.ReportTask, required: bool=True, allow_empty: bool=True):
    self.task = task
    self.allow_empty = allow_empty
    inserted_row_count_query = SQLQuery(
      query=f'''
select count(*) as row_count 
from {self.task.report_table_model.full_table_name}
where fetch_date = %s;
      ''',
      substitution_parameters=(SQLQuery.format_time(self.task.run_date),)
    )
    report_row_count_stream = StringIO()
    pd.DataFrame([{
      'row_count': self.task.row_count if self.task.row_count is not None else 0,
    }]).to_csv(report_row_count_stream, index=False)
    report_row_count_stream.seek(0)
    super().__init__(
      identifier='row_count',
      configuration={
        'required': required,
        'after': True,
        'verify': {
          'name_a': 'inserted_row_count',
          'text_a': inserted_row_count_query.substituted_query,
          'name_b': 'report_row_count',
          'stream_b': report_row_count_stream,
          'csv_b': True,
        }
      }
    )

  def result_description(self, result: Verifier.Verification) -> str:
    return f'{self.task.row_count} report rows. {super().result_description(result=result)}'

  def verify_task(self, task: tasks.ReportTask) -> Verification.VerificationResult:
    assert task is self.task
    if task.row_count is None:
      return Verification.VerificationResult(
        identifier=self.identifier,
        success=self.allow_empty,
        required=self.required,
        description='No report rows.'
      )
    return super().verify_task(task=task)
