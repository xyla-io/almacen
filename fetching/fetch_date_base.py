import models
import tasks
import sqlalchemy as alchemy

from datetime import datetime, date, timedelta
from typing import TypeVar, Generic, Dict, Optional

def sql_max(a: Optional[any], b: Optional[any]) -> Optional[any]:
  if a is None:
    return b
  elif b is None:
    return a
  else:
    return max(a, b)

def sql_min(a: Optional[any], b: Optional[any]) -> Optional[any]:
  if a is None:
    return b
  elif b is None:
    return a
  else:
    return min(a, b)

T = TypeVar(tasks.ReportTask)
class ReportDateFetcher(Generic[T]):
  task: T

  def __init__(self, task: T):
    self.task = task

  @property
  def ripe_data_age(self) -> timedelta:
    return timedelta(seconds=0)

  @property
  def report_increment(self) -> timedelta:
    return timedelta(days=1)

  @property
  def report_interval(self) -> timedelta:
    return timedelta(days=6)

  @property
  def max_ripe_date(self) -> datetime:
   return self.clamped_date(models.TimeModel.shared.utc_now - self.ripe_data_age)

  def clamped_date(self, date: datetime) -> datetime:
    return datetime.combine(date, datetime.min.time())

  def backfill_start_date(self, backfill_target_date: datetime, min_date_fetched: Optional[datetime]=None) -> datetime:
    if min_date_fetched is None:
      return self.clamped_date(backfill_target_date)
    return max(backfill_target_date, self.clamped_date(min_date_fetched) - self.report_increment - self.report_interval)
  
  def backfill_end_date(self, backfill_target_date: datetime, min_date_fetched: Optional[datetime]=None) -> datetime:
    if min_date_fetched is None:
      return min(self.clamped_date(backfill_target_date) + self.report_interval, self.max_ripe_date)
    end_date = self.clamped_date(min_date_fetched) - self.report_increment
    return backfill_target_date - timedelta(days=1) if end_date > self.max_ripe_date else end_date

  def report_start_date(self, max_date_fetched: Optional[datetime]) -> datetime:
    if max_date_fetched is None:
      return self.max_ripe_date - self.report_interval
    return self.clamped_date(max_date_fetched) + self.report_increment
    
  def report_end_date(self, max_date_fetched: Optional[datetime]) -> datetime:
    if max_date_fetched is None:
      return self.max_ripe_date
    return min(self.clamped_date(max_date_fetched) + self.report_increment + self.report_interval, self.max_ripe_date)

  def handle_report_table_does_not_exist(self):
    backfill_target = models.TimeModel.shared.backfill_target_date
    if backfill_target is None:
      self.task.report_start_date = self.report_start_date(max_date_fetched=None)
      self.task.report_end_date = self.report_end_date(max_date_fetched=None)
    else:
      self.task.report_start_date = self.backfill_start_date(backfill_target_date=backfill_target)
      self.task.report_end_date = self.backfill_end_date(backfill_target_date=backfill_target)

  def align_dates_to_increment(self):
    increment = self.report_increment.total_seconds()
    if increment == 0 or self.task.report_start_date > self.task.report_end_date:
      return

    interval = (self.task.report_end_date - self.task.report_start_date).total_seconds()
    increments = int(interval / increment)
    rounded_interval = timedelta(seconds=increments * increment)
    
    if models.TimeModel.shared.backfill_target_date is None:
      self.task.report_end_date = self.task.report_start_date + rounded_interval
    else:
      self.task.report_start_date = self.task.report_end_date - rounded_interval

  def override_dates_with_time_model(self):
    if models.TimeModel.start_date is not None:
      print(f'Overriding start date from time model:\n{self.task.report_start_date} -> {models.TimeModel.start_date}')
      self.task.report_start_date = models.TimeModel.start_date
    if models.TimeModel.end_date is not None:
      print(f'Overriding end date from time model:\n{self.task.report_end_date} -> {models.TimeModel.end_date}')
      self.task.report_end_date = models.TimeModel.end_date

  def fetch(self):
    self.task.run_date = models.TimeModel.shared.utc_now
    if not self.task.report_table_exists:
      self.handle_report_table_does_not_exist()
      self.override_dates_with_time_model()
      return

    backfill_target = models.TimeModel.shared.backfill_target_date

    session = self.task.sql_layer.alchemy_session()
    date_column = self.task.report_table_model.table.columns[self.task.report_table_model.date_column_name]
    query = session.query(alchemy.func.max(date_column)) if backfill_target is None else session.query(alchemy.func.min(date_column))
    query = self.task.filtered_alchemy_query_by_identifier_columns(query=query) \
      .filter(self.task.report_table_model.table.columns[self.task.report_table_model.crystallized_column_name] == True)
      
    max_or_min_date_result = query.one()[0]
    if isinstance(max_or_min_date_result, date):
      max_or_min_date_result = datetime.combine(max_or_min_date_result, datetime.min.time())

    if backfill_target is None:
      if self.task.last_run_history is not None and self.task.last_run_history.target_end_time is not None and self.task.last_run_history.status == 'completed':
        max_or_min_date_result = sql_max(max_or_min_date_result, self.task.last_run_history.target_end_time)

      self.task.report_start_date = self.report_start_date(max_date_fetched=max_or_min_date_result)
      self.task.report_end_date = self.report_end_date(max_date_fetched=max_or_min_date_result)
    else:
      if self.task.last_run_history is not None and self.task.last_run_history.target_start_time is not None and self.task.last_run_history.status == 'completed':
        max_or_min_date_result = sql_min(max_or_min_date_result, self.task.last_run_history.target_start_time)

      self.task.report_start_date = self.backfill_start_date(
        backfill_target_date=backfill_target,
        min_date_fetched=max_or_min_date_result
      )
      self.task.report_end_date = self.backfill_end_date(
        backfill_target_date=backfill_target,
        min_date_fetched=max_or_min_date_result
      )
    
    self.align_dates_to_increment()
    self.override_dates_with_time_model()

class BaseReportDateFetcher(ReportDateFetcher[tasks.ReportTask]):
  pass

class CurrentDateFetcher(BaseReportDateFetcher):
  def fetch(self):
    self.task.run_date = models.TimeModel.shared.utc_now
    self.task.report_start_date = self.task.run_date
    self.task.report_end_date = self.task.run_date

U = TypeVar(tasks.MutateReportTask)
class MutatorDateFetcher(Generic[U], ReportDateFetcher[U]):
  @property
  def ripe_data_age(self) -> timedelta:
    return timedelta(days=0)

class BaseMutatorDateFetcher(MutatorDateFetcher[tasks.MutateReportTask]):
  pass

class MaterializeDateFetcher(Generic[U], MutatorDateFetcher[U]):
  @property
  def report_increment(self) -> timedelta:
    return timedelta(days=-3) if models.TimeModel.shared.backfill_target_date is not None else timedelta(days=-43) 

  @property
  def report_interval(self) -> timedelta:
    return timedelta(days=60)

  def align_dates_to_increment(self):
    pass

class BaseMaterializeDateFetcher(MaterializeDateFetcher[tasks.MutateReportTask]):
  pass