import models
import tasks
import sqlalchemy as alchemy

from . import fetch_date_base
from datetime import datetime, timedelta

class AppsFlyerReportDateFetcher(fetch_date_base.ReportDateFetcher):
  @property
  def report_interval(self) -> timedelta:
    return timedelta(days=2)

class AppsFlyerInstallsDateFetcher(fetch_date_base.ReportDateFetcher):
  @property
  def report_interval(self) -> timedelta:
    return timedelta(days=2)

class AppsFlyerCustomEventDateFetcher(fetch_date_base.ReportDateFetcher):
  @property
  def report_interval(self) -> timedelta:
    return timedelta(days=4)

class AppsFlyerDataLockerDateFetcher(fetch_date_base.ReportDateFetcher):
  @property
  def report_interval(self) -> timedelta:
    return timedelta(days=2)

  @property
  def report_increment(self) -> timedelta:
    return timedelta(seconds=0)

  def fetch(self):
    super().fetch()

    backfill_target = models.TimeModel.shared.backfill_target_date
    session = self.task.sql_layer.alchemy_session()
    query = session.query(
      self.task.report_table_model.table.columns.data_locker_hour,
      alchemy.func.max(self.task.report_table_model.table.columns.data_locker_end_part)
    )
    query = self.task.filtered_alchemy_query_by_identifier_columns(query=query) \
      .filter(self.task.report_table_model.table.columns[self.task.report_table_model.date_column_name] == (self.task.report_start_date if backfill_target is None else self.task.report_end_date)) \
      .group_by(self.task.report_table_model.table.columns.data_locker_hour) \
      .order_by(self.task.report_table_model.table.columns.data_locker_hour.desc()) \
      .limit(1)
      
    max_part_result = query.one_or_none()
    if max_part_result is not None:
      self.task.report_start_hour = max_part_result[0]
      self.task.report_start_part = max_part_result[1] + 1
    else:
      self.task.report_start_hour = 0
      self.task.report_start_part = 0