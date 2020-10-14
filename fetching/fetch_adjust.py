import pandas as pd
import tasks
import sys

from . import base
from datetime import datetime, timedelta
from typing import TypeVar, Generic
from garfield import AdjustReporter, AdjustReportPeriod
from math import ceil

T = TypeVar(tasks.FetchAdjustReportTask)
class AdjustReportFetcher(Generic[T], base.ReportFetcher[T]):
  pass

class AdjustDeliverablesReportFetcher(AdjustReportFetcher[tasks.FetchAdjustDeliverablesReportTask]):
  def fetch(self):
    reporter = AdjustReporter(api=self.task.api)

    date = self.task.report_start_date
    report = None
    while date <= self.task.report_end_date:
      date_report = reporter.fetch_deliverables_report(
        start_date=date,
        end_date=date
      )
      date_report['date'] = date
      if report is None:
        report = date_report
      else:
        report = report.append(date_report)
      date = date + timedelta(days=1)

    if report is not None:
      report.reset_index(drop=True, inplace=True)

    self.task.report = report

class AdjustEventsReportFetcher(AdjustReportFetcher[tasks.FetchAdjustEventsReportTask]):
  def fetch(self):
    reporter = AdjustReporter(api=self.task.api)

    date = self.task.report_start_date
    report = None
    while date <= self.task.report_end_date:
      date_report = reporter.fetch_events_report(
        start_date=date,
        end_date=date
      )
      date_report['date'] = date
      if report is None:
        report = date_report
      else:
        report = report.append(date_report)
      date = date + timedelta(days=1)

    if report is not None:
      report.reset_index(drop=True, inplace=True)

    self.task.report = report

class AdjustCohortsMeasuresReportFetcher(AdjustReportFetcher[tasks.FetchAdjustCohortsMeasuresReportTask]):
  def fetch(self):
    reporter = AdjustReporter(api=self.task.api)

    cohort = self.task.first_cohort_date
    if self.task.period.max_period is not None:
      first_relevant_date = self.task.report_start_date - timedelta(days=self.task.period.days * self.task.period.max_period)
      irrelevant_cohorts = ceil((first_relevant_date - cohort).days / self.task.cohort_days)
      if irrelevant_cohorts > 0:
        cohort += timedelta(days=self.task.cohort_days * irrelevant_cohorts)

    report = None
    cohort_number = 0
    cohort_interval = timedelta(days=self.task.cohort_days - 1)
    while cohort <= self.task.report_end_date:
      cohort_number += 1
      if not cohort_number % 10:
        print(f'Fetching cohort number {cohort_number}: {cohort.strftime("%Y-%m-%d")}')
        sys.stdout.flush()
      cohort_report = reporter.fetch_cohort_report(
        cohort_start=cohort,
        cohort_end=cohort + cohort_interval,
        period=self.task.period,
      )
      cohort_report['cohort'] = cohort

      # filter out rows that are outside the task date range
      filter_report = cohort_report[['cohort', 'period']].copy()
      filter_report['effective_date'] = filter_report.cohort + filter_report.period.apply(lambda p: timedelta(days=p * self.task.period.days))
      filter_report.drop(filter_report.index[filter_report.effective_date < self.task.report_start_date], inplace=True)
      filter_report.drop(filter_report.index[filter_report.effective_date > self.task.report_end_date], inplace=True)
      cohort_report = cohort_report.loc[filter_report.index]
      
      if report is None:
        report = cohort_report
      else:
        report = report.append(cohort_report)
      cohort = cohort + timedelta(days=self.task.cohort_days)

    if report is not None:
      report.reset_index(drop=True, inplace=True)

    self.task.report = report
