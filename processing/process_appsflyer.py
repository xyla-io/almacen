import tasks
import models
import pandas as pd

from . import base
from math import floor
from typing import Dict
from datetime import datetime, timedelta

class AppsFlyerReportProcessor(base.ReportProcessor[tasks.FetchAppsFlyerReportTask]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'app_id': self.task.app_id,
    }

  def process(self):
    report = self.task.report
    report_row_count = report.shape[0]
    if report_row_count >= 200000:
      days = (self.task.report_end_date - self.task.report_start_date).days + 1
      retry_days = 2 if days == 3 else floor(days / 2)
      if retry_days > 0:
        self.task.report_end_date = self.task.report_start_date + timedelta(days=retry_days - 1)
        self.task.behaviors = [b for b in self.task.behaviors if b.behavior_type is not models.ReportTaskBehaviorType.fetch_date]
        self.task.retry = f'reduce days from {days} to {retry_days}'
      raise ValueError('Report row count too high; the data from the API may have been truncated.', report_row_count)
    
    if not report.empty:
      boolean_columns = [
        'Is Retargeting',
        'Is Primary Attribution',
      ]
      for column in boolean_columns:
        report[column] = report[column].map({'true': True, 'false': False})
    
    super().process()

class AppsFlyerDataLockerHourPartReportProcessor(base.ReportProcessor[tasks.FetchAppsFlyerDataLockerHourPartReportTask]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'data_locker_report_type': self.task.data_locker_report_type,
      'data_locker_hour': self.task.hour_value,
      'data_locker_start_part': self.task.part,
      'data_locker_end_part': self.task.part,
      'data_locker_last_part': self.task.last_part,
    }

  def process(self):
    report = self.task.report
    report.fillna('', inplace=True)

    if self.task.format_integer_id_columns:
      self._fix_id_column_integer_format(report=report)

    report['attributed_touch_date'] = report['attributed_touch_time'].apply(lambda s: s[:10] if s else '')
    report['install_date'] = report['install_time'].apply(lambda s: s[:10] if s else '')
    report['event_date'] = report['event_time'].apply(lambda s: s[:10] if s else '')

    time_format = self.task.report_table_model.date_format_string
    int_nan_placeholder = models.NullPlaceholder.integer.value

    report['event_day'] = report.event_time.apply(lambda s: datetime.strptime(s, time_format)) - pd.to_datetime(report.attributed_touch_time.apply(lambda s: datetime.strptime(s, time_format) if s else None))
    report.event_day = report.event_day.apply(lambda t: int(t.delta / 60 / 60 / 24 / 1000000000) if not pd.isna(t) else int_nan_placeholder)

    report['revenue'] = report['event_revenue_usd'].apply(lambda s: float(s) if s else 0)
    report['agency'] = report.af_prt if 'af_prt' in report.columns else ''

    report = report.groupby(
      [
        'app_id',
        'bundle_id',
        'app_name',
        'app_version',
        'agency',
        'media_source',
        'af_channel',
        'campaign',
        'af_adset',
        'af_ad',
        'af_c_id',
        'af_adset_id',
        'af_ad_id',
        'af_keywords',
        'keyword_match_type',
        'country_code',
        'is_primary_attribution',
        'is_retargeting',
        'retargeting_conversion_type',
        'platform',
        'event_name',
        'attributed_touch_date',
        'install_date',
        'event_date',
        'event_day',
      ], sort=False).agg(
        {
          'revenue': 'sum', 
          'event_name': 'count',
        }
      )
    report.rename(columns={'event_name': 'events'}, inplace=True)
    report.reset_index(inplace=True)
    report.rename(columns={
        'af_channel': 'channel',
        'af_adset': 'adset',
        'af_ad': 'ad',
        'af_c_id': 'campaign_id',
        'af_adset_id': 'adset_id',
        'af_ad_id': 'ad_id',
        'af_keywords': 'keyword',
      },
      inplace=True
    )
    report['data_locker_date'] = self.task.date
    self.task.report = report
    super().process()

    parts_report = self.task.report_parts_context.report
    parts_report = parts_report.append(self.task.report)
    parts_report = parts_report.groupby(
      [
        'app_id',
        'bundle_id',
        'app_name',
        'app_version',
        'agency',
        'media_source',
        'channel',
        'campaign',
        'adset',
        'ad',
        'campaign_id',
        'adset_id',
        'ad_id',
        'keyword',
        'keyword_match_type',
        'country_code',
        'is_primary_attribution',
        'is_retargeting',
        'retargeting_conversion_type',
        'platform',
        'event_name',
        'attributed_touch_date',
        'install_date',
        'event_date',
        'event_day',
        'company_display_name',
        'data_locker_report_type',
        'data_locker_date',
        'data_locker_hour',
      ], sort=False).agg(
        {
          'revenue': 'sum', 
          'events': 'sum',
          'data_locker_start_part': 'min',
          'data_locker_end_part': 'max',
          'data_locker_last_part': 'max',
        }
      )
    parts_report.reset_index(inplace=True)
    parts_report['effective_date'] = parts_report.apply(lambda r: datetime.strptime(r.attributed_touch_date, '%Y-%m-%d') + timedelta(days=r.event_day) if r.attributed_touch_date else r.event_date, axis=1)
    self.task.report = parts_report 
    self.task.report_parts_context.report = parts_report

  def _fix_id_column_integer_format(self, report: pd.DataFrame):
    id_columns = [
      'af_c_id',
      'af_adset_id',
      'af_ad_id',
    ]
    for id_column in id_columns:
      report[id_column] = report[id_column].apply(lambda s: str(pd.to_numeric(s, errors='ignore', downcast='integer')) if s else '')
