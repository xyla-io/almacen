import tasks
import models

from . import base
from datetime import timedelta
from salem import AppsFlyerReporter, AppsFlyerDataLockerReporter

class AppsFlyerReportFetcher(base.ReportFetcher[tasks.FetchAppsFlyerReportTask]):
  pass

class AppsFlyerPurchaseEventsReportFetcher(AppsFlyerReportFetcher):
  def fetch(self):
    reporter = AppsFlyerReporter(api=self.task.api)
    self.task.report = reporter.get_events_report(
      start_date=self.task.report_start_date, 
      end_date=self.task.report_end_date,
      event_names=['af_purchase'],
      columns=self.task.fetch_columns,
    )

    return 'Values fetched:\n{}'.format(self.task.report.count())

class AppsFlyerInstallEventsReportFetcher(AppsFlyerReportFetcher):
  def fetch(self):
    reporter = AppsFlyerReporter(api=self.task.api)
    self.task.report = reporter.get_installs_report(
      start_date=self.task.report_start_date, 
      end_date=self.task.report_end_date,
      columns=self.task.fetch_columns,
    )

    return 'Values fetched:\n{}'.format(self.task.report.count())

class AppsFlyerCustomEventsReportFetcher(AppsFlyerReportFetcher):
  def fetch(self):
    reporter = AppsFlyerReporter(api=self.task.api)
    self.task.report = reporter.get_events_report(
      start_date=self.task.report_start_date, 
      end_date=self.task.report_end_date,
      event_names=self.task.custom_event_names,
      columns=self.task.fetch_columns,
    )

    return 'Values fetched:\n{}'.format(self.task.report.count())

class AppsFlyerDataLockerHourPartReportFetcher(base.ReportFetcher[tasks.FetchAppsFlyerDataLockerHourPartReportTask]):
  def fetch(self):
    reporter = AppsFlyerDataLockerReporter(api=self.task.api)
    self.task.report = reporter.get_hour_part_report(
      event_name=self.task.data_locker_report_type, 
      date=self.task.date, 
      hour_value=self.task.hour_value, 
      part=self.task.part
    )

class AppsFlyerDataLockerReportFetcher(base.ReportFetcher[tasks.FetchAppsFlyerDataLockerReportTask]):
  def fetch(self):
    is_backfill = models.TimeModel.shared.backfill_target_date is not None
    reporter = AppsFlyerDataLockerReporter(api=self.task.api)
    days = (self.task.report_end_date - self.task.report_start_date).days + 1
    date_range = [self.task.report_start_date + timedelta(days=d) for d in range(0, days)]
    if is_backfill:
      date_range.reverse()
    hour_range = [h for h in range(0, 25)]

    hour_configurations = [
      {
        'date': d,
        'hour_value': h,
      }
      for d in date_range
      for h in hour_range
    ]
    self.task.hour_part_subtask_configurations = []
    for hour_configuration in hour_configurations:
      metadata = reporter.get_hour_part_metadata(
        event_name=self.task.data_locker_report_type, 
        date=hour_configuration['date'], 
        hour_value=hour_configuration['hour_value']
      )
      if not metadata['completed']: break # the _SUCCESS file is not presetn
      if not metadata['part_count']: # the _SUCCESS file is present but no part files exist
        self.task.hour_part_subtask_configurations += [
          {
            **hour_configuration,
            'part': -1,
            'last_part': -1,
          }
        ]
      else: # the _SUCCESS file and one or more part files exist
        self.task.hour_part_subtask_configurations += [
          {
            **hour_configuration,
            'part': p,
            'last_part': metadata['part_count'] - 1,
          }
          for p in range(0, metadata['part_count'])
        ]
    self.filter_subtask_configurations()
    
  def filter_subtask_configurations(self):
    is_backfill = models.TimeModel.shared.backfill_target_date is not None

    all_dates = sorted({
      p['date']
      for p in self.task.hour_part_subtask_configurations
    })
    if is_backfill:
      all_dates.reverse()
    complete_dates = {
      p['date']
      for p in self.task.hour_part_subtask_configurations if p['hour_value'] == 24
    }
    valid_dates = []
    for date in all_dates:
      valid_dates.append(date)
      if date not in complete_dates:
        break

    if is_backfill:
      self.task.report_start_date = valid_dates[-1] if valid_dates else self.task.report_end_date + timedelta(days=1)
    else:
      self.task.report_end_date = valid_dates[-1] if valid_dates else self.task.report_start_date - timedelta(days=1)

    overlap_date = self.task.report_end_date if is_backfill else self.task.report_start_date
    self.task.hour_part_subtask_configurations = [
      c for c in self.task.hour_part_subtask_configurations
      if c['last_part'] >= 0 # part files exist
        and c['date'] in valid_dates # dates except for the last must be complete
        and (c['date'] != overlap_date # no hour parts for the date have been fetched before
        or c['hour_value'] > self.task.report_start_hour # these hours for the first date have not been fetched before
        or (c['hour_value'] == self.task.report_start_hour and c['part'] >= self.task.report_start_part)) # these parts for the first hour have not been fetched before
    ]
    