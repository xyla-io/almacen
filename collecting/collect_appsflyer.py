import tasks
import sqlalchemy as alchemy
import pandas as pd

from . import base
from models import NullPlaceholder
from typing import Dict, Optional

class AppsFlyerReportCollector(base.ReportCollector[tasks.FetchAppsFlyerReportTask]):
  pass
  
class AppsFlyerDataLockerReportCollector(base.ReportCollector[tasks.FetchAppsFlyerDataLockerReportBaseTask]):
  def collect(self):
    report = self.task.report
    partial_report = pd.DataFrame()

    if self.task.collection_threshold is not None:
      if len(report) < self.task.collection_threshold:
        self.task.report_parts_context.report = report
        self.task.report = None
        return f'Row count {len(report)} is less than threshold ({self.task.collection_threshold}) and will not be collected yet'

      hour_report = report[['data_locker_date', 'data_locker_hour']]
      hour_report = hour_report.groupby(['data_locker_date', 'data_locker_hour'], sort=False).size().to_frame(name='count')
      hour_report.reset_index(inplace=True)
      if len(hour_report) > 1:
        last_date = hour_report.data_locker_date.max()
        last_hour = hour_report[hour_report.data_locker_date == last_date].data_locker_hour.max()
        partial_report = report[(report.data_locker_date == last_date) & (report.data_locker_hour == last_hour)]
        report.drop(report.index[(report.data_locker_date == last_date) & (report.data_locker_hour == last_hour)], inplace=True)

    row_count = len(self.task.report)
    report.event_day = report.event_day.apply(lambda n: None if n == NullPlaceholder.integer.value else n).astype(pd.Int64Dtype())
    report['fetch_date'] = self.task.report_parts_context.run_date
    super().collect()
    self.task.report_parts_context.report = partial_report
    self.task.report = None
    return f'{row_count} rows collected'