import pandas as pd
import tasks

from . import base
from models import TimeModel
from datetime import datetime, timedelta
from azrael import SnapchatReporter
from typing import Optional

class SnapchatReportFetcher(base.ReportFetcher[tasks.FetchSnapchatReportTask]):
  api_start_date: Optional[datetime]
  api_end_date: Optional[datetime]

  def fetch(self):
    self.task.api.ad_account_id = self.task.ad_account_id
    self.task.campaigns = self.task.api.get_campaigns()
    self.task.ad_squads = self.task.api.get_ad_squads()

    reporter = SnapchatReporter(api=self.task.api)

    self.api_start_date = reporter.clamped_date_in_account_timezone(
      date=self.task.report_start_date,
      now=TimeModel.shared.utc_now
    )
    self.api_end_date = reporter.clamped_date_in_account_timezone(
      date=self.task.report_end_date + timedelta(days=1),
      now=TimeModel.shared.utc_now
    )

    print(f'Actual start: {self.api_start_date}')
    print(f'Actual end: {self.api_end_date}')

    start_day_offest = (self.api_start_date - self.task.report_start_date).days
    self.task.report_start_date = self.api_start_date - timedelta(days=start_day_offest, hours=self.api_start_date.hour)
    self.task.report_end_date = self.api_end_date - timedelta(days=start_day_offest + 1, hours=self.api_end_date.hour)

class SnapchatCampaignsReportFetcher(SnapchatReportFetcher):
  def fetch(self):
    super().fetch()
    if self.task.report_start_date > self.task.report_end_date:
      return
    
    reporter = SnapchatReporter(api=self.task.api)
    df = pd.DataFrame()

    for c in self.task.campaigns:
      report = reporter.get_campaign_stats(
        campaign_id=c['id'],
        start_date=self.api_start_date,
        end_date=self.api_end_date,
        columns=self.task.fetch_columns,
        swipe_up_attribution_window=self.task.swipe_up_attribution_window,
        view_attribution_window=self.task.view_attribution_window
      )
      df = df.append(report, sort=False)

    df[self.task.fetched_currency_column] = self.task.api.ad_account['currency']
    self.task.report = df
    

class SnapchatAdSquadsReportFetcher(SnapchatReportFetcher):
  def fetch(self):
    super().fetch()
    if self.task.report_start_date > self.task.report_end_date:
      return

    reporter = SnapchatReporter(api=self.task.api)
    df = pd.DataFrame()

    for c in self.task.campaigns:
      report = reporter.get_adsquad_stats(
        campaign_id=c['id'],
        start_date=self.api_start_date,
        end_date=self.api_end_date,
        columns=self.task.fetch_columns,
        swipe_up_attribution_window=self.task.swipe_up_attribution_window,
        view_attribution_window=self.task.view_attribution_window
      )
      df = df.append(report, sort=False)
    
    df[self.task.fetched_currency_column] = self.task.api.ad_account['currency']
    self.task.report = df
    

class SnapchatAdsReportFetcher(SnapchatReportFetcher):
  def fetch(self):
    super().fetch()
    if self.task.report_start_date > self.task.report_end_date:
      return

    self.task.ads = self.task.api.get_ads()
    reporter = SnapchatReporter(api=self.task.api)
    df = pd.DataFrame()

    for c in self.task.campaigns:
      report = reporter.get_ad_stats(
        campaign_id=c['id'],
        start_date=self.api_start_date,
        end_date=self.api_end_date,
        columns=self.task.fetch_columns,
        swipe_up_attribution_window=self.task.swipe_up_attribution_window,
        view_attribution_window=self.task.view_attribution_window
      )
      df = df.append(report, sort=False)
    
    df[self.task.fetched_currency_column] = self.task.api.ad_account['currency']
    self.task.report = df