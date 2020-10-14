import tasks
import models
import sqlalchemy as alchemy

from datetime import datetime, timedelta
from . import fetch_date_base

class PerformanceCubeDateFetcherMaterializeDateFetcher(fetch_date_base.MaterializeDateFetcher[tasks.MaterializePerformanceCubeUnfilteredBaseTask]):
  @property
  def report_increment(self) -> timedelta:
    return timedelta(days=-self.task.backfill_rematerialize_days) if models.TimeModel.shared.backfill_target_date is not None else timedelta(days=-self.task.rematerialize_days) 

  @property
  def report_interval(self) -> timedelta:
    return timedelta(days=self.task.materialize_days)

class PerformanceCubeGlobalDateFetcher(fetch_date_base.ReportDateFetcher[tasks.MutatePerformanceCubeGlobalTask]):
  def fetch(self):
    session = self.task.sql_layer.alchemy_session()
    date_column = self.task.report_table_model.table.columns[self.task.report_table_model.date_column_name]
    query = session.query(alchemy.func.min(date_column), alchemy.func.max(date_column))
    query = self.task.filtered_alchemy_query_by_identifier_columns(query=query)
      
    date_range_result = query.one_or_none()
    if date_range_result is not None and date_range_result[0] is not None and date_range_result[1] is not None:
      self.task.report_start_date = datetime.combine(date_range_result[0], datetime.min.time())
      self.task.report_end_date = datetime.combine(date_range_result[1], datetime.min.time())
    else:
      self.task.report_start_date = datetime.combine(models.TimeModel.shared.utc_now.date(), datetime.min.time())
      self.task.report_end_date = self.task.report_start_date - timedelta(days=1)
      