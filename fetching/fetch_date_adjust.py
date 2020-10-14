import tasks
import models

from . import fetch_date_base
from datetime import datetime, timedelta
from typing import Optional, Generic, TypeVar

T = TypeVar(tasks.FetchAdjustReportTask)
class AdjustReportDateFetcher(Generic[T], fetch_date_base.ReportDateFetcher[T]):
  pass
  
U = TypeVar(tasks.FetchAdjustCohortsMeasuresReportTask)
class AdjustCohortsMeasuresReportDateFetcher(Generic[U], AdjustReportDateFetcher[U]):
  def report_start_date(self, max_date_fetched: Optional[datetime]) -> datetime:
    if max_date_fetched is None:
      return self.task.first_cohort_date
    
    return super().report_start_date(max_date_fetched=max_date_fetched)

  def report_end_date(self, max_date_fetched: Optional[datetime]) -> datetime:
    return self.clamped_date(self.task.run_date - self.ripe_data_age)
