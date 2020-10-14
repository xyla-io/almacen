import tasks
import pandas as pd

from . import base
from jones import FixerReporter
from datetime import datetime

class CurrencyExchangeReportFetcher(base.ReportFetcher[tasks.FetchBaseCurrencyExchangeReportTask]):
  def fetch(self):
    reporter = FixerReporter(api=self.task.api)
    report = pd.DataFrame()
    date_range = pd.date_range(
      start=self.task.report_start_date,
      end=self.task.report_end_date,
    )
    date_range = [datetime.combine(d.date(), datetime.min.time()) for d in date_range]
    for date in date_range:
      to_currencies = [
        c for c in self.task.currencies
        if (c not in self.task.min_currency_dates_to_fetch or date >= self.task.min_currency_dates_to_fetch[c])
          and (c not in self.task.max_currency_dates_to_fetch or date <= self.task.max_currency_dates_to_fetch[c])
      ]
      date_report = reporter.get_exchange_rate_report(
        date=date,
        from_currency=self.task.base_currency, 
        to_currencies=to_currencies
      )
      report = report.append(date_report)

    self.task.report = report
