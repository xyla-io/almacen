import models
import sqlalchemy as alchemy

from . import fetch_date_base
from datetime import datetime, timedelta

class CurrencyExchangeReportDateFetcher(fetch_date_base.ReportDateFetcher):
  def fetch(self):
    self.task.run_date = models.TimeModel.shared.utc_now
    if not self.task.report_table_exists:
      self.handle_report_table_does_not_exist()
      return

    backfill_target = models.TimeModel.shared.backfill_target_date

    session = self.task.sql_layer.alchemy_session()
    date_column = self.task.report_table_model.table.columns[self.task.report_table_model.date_column_name]
    target_column = self.task.report_table_model.table.columns['target']
    crystallized_column = self.task.report_table_model.table.columns[self.task.report_table_model.crystallized_column_name]
    query = session.query(target_column, alchemy.func.max(date_column).label('max_date')).group_by(target_column) if backfill_target is None else session.query(target_column, alchemy.func.min(date_column).label('min_date')).group_by(target_column)

    query = self.task.filtered_alchemy_query_by_identifier_columns(query=query).filter(crystallized_column == True)
    results = query.all()

    if backfill_target is None:
      max_currency_dates_fetched = {r.target: datetime.combine(r.max_date, datetime.min.time()) for r in results}
      max_date_fetched = min(max_currency_dates_fetched.values()) if max_currency_dates_fetched else None
      self.task.min_currency_dates_to_fetch = {
        k: max_currency_dates_fetched[k] + timedelta(days=1) 
        for k in max_currency_dates_fetched
      }
      self.task.report_start_date = self.report_start_date(max_date_fetched=max_date_fetched)
      self.task.report_end_date = self.report_end_date(max_date_fetched=max_date_fetched)
    else:
      min_currency_dates_fetched = {r.target: datetime.combine(r.min_date, datetime.min.time()) for r in results}
      min_date_fetched = max(min_currency_dates_fetched.values()) if min_currency_dates_fetched else None
      self.task.max_currency_dates_to_fetch = {
        k: min_currency_dates_fetched[k] - timedelta(days=1) 
        for k in min_currency_dates_fetched
      }
      self.task.report_start_date = self.backfill_start_date(
        backfill_target_date=backfill_target,
        min_date_fetched=min_date_fetched
      )
      self.task.report_end_date = self.backfill_end_date(
        backfill_target_date=backfill_target,
        min_date_fetched=min_date_fetched
      )
    
    self.align_dates_to_increment()
    self.override_dates_with_time_model()