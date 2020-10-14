import pandas as pd
import tasks

from . import base
from . import fetch_date_base
from typing import Dict
from datetime import datetime
from heathcliff import SearchAdsReporter

class AppleReportFetcher(base.ReportFetcher[tasks.FetchAppleReportTask]):
  def fetch(self):
    self.task.org_name = self.task.api.get_org_name()

class AppleCampaignsReportFetcher(AppleReportFetcher):
  def fetch(self):
    super().fetch()

    reporter = SearchAdsReporter(api=self.task.api)
    self.task.report = reporter.get_campaigns_report(
      start_date=self.task.report_start_date, 
      end_date=self.task.report_end_date
    )

class AppleAdGroupsReportFetcher(AppleReportFetcher):
  def fetch(self):
    super().fetch()

    reporter = SearchAdsReporter(api=self.task.api)
    self.task.report = reporter.get_adgroups_report(
      start_date=self.task.report_start_date, 
      end_date=self.task.report_end_date
    )

class AppleKeywordsReportFetcher(AppleReportFetcher):
  def fetch(self):
    super().fetch()

    reporter = SearchAdsReporter(api=self.task.api)
    self.task.report = reporter.get_keywords_report(
      start_date=self.task.report_start_date, 
      end_date=self.task.report_end_date
    )

class AppleCreativeSetsReportFetcher(AppleReportFetcher):
  def fetch(self):
    super().fetch()

    reporter = SearchAdsReporter(api=self.task.api)
    self.task.report = reporter.get_creative_sets_report(
      start_date=self.task.report_start_date, 
      end_date=self.task.report_end_date
    )

class AppleSearchTermsReportFetcher(AppleReportFetcher):
  def fetch(self):
    super().fetch()

    reporter = SearchAdsReporter(api=self.task.api)
    self.task.report = reporter.get_searchterms_report(
      start_date=self.task.report_start_date, 
      end_date=self.task.report_end_date
    )