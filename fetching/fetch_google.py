import tasks
import pandas as pd

from . import base
from hazel import GoogleAdsReporter
from typing import Optional, List

# ---------------------------------------------------------------------------
# Google Ads
# ---------------------------------------------------------------------------
class GoogleAdsReportFetcher(base.ReportFetcher[tasks.FetchGoogleAdsMultiCustomerReportTask]):
  def fetch(self) -> Optional[any]:
    print('Retrieving customer IDs...')
    customer_ids = self.task.api.get_customer_ids(exclude_customers=self.task.exclude_customer_ids)
    print(f'Found: {len(customer_ids)} IDs')
    print('Filtering out manager accounts...')
    self.task.customer_ids = [i for i in customer_ids if not self.task.api.customer_is_manager(i)]
    print(f'Remaining IDs ({len(self.task.customer_ids)}): {self.task.customer_ids}')


class GoogleAdsAdConversionActionsCustomerReportFetcher(base.ReportFetcher[tasks.FetchGoogleAdsAdConversionActionsCustomerReportTask]):
  def fetch(self) -> Optional[any]:
    reporter = GoogleAdsReporter(api=self.task.api)
    self.task.report = reporter.get_ad_conversion_action_report(
      start_date=self.task.report_start_date,
      end_date=self.task.report_end_date,
      customer_id=self.task.customer_id
    )
    return f'Values fetched: {self.task.report.count()}'

class GoogleAdsAssetsCustomerReportFetcher(base.ReportFetcher[tasks.FetchGoogleAdsAssetsCustomerReportTask]):
  def fetch(self) -> Optional[any]:
    reporter = GoogleAdsReporter(api=self.task.api)
    self.task.report = reporter.get_asset_report(
      customer_id=self.task.customer_id
    )
    return f'Values fetched: {self.task.report.count()}'

class GoogleAdsAdsCustomerReportFetcher(base.ReportFetcher[tasks.FetchGoogleAdsAdsCustomerReportTask]):
  def fetch(self) -> Optional[any]:
    reporter = GoogleAdsReporter(api=self.task.api)
    self.task.report = reporter.get_ad_report(
      start_date=self.task.report_start_date,
      end_date=self.task.report_end_date,
      customer_id=self.task.customer_id,
      json_encode_repeated=False
    )
    return f'Values fetched: {self.task.report.count()}'

class GoogleAdsAdAssetsCustomerReportFetcher(base.ReportFetcher[tasks.FetchGoogleAdsAdAssetsCustomerReportTask]):
  def fetch(self) -> Optional[any]:
    reporter = GoogleAdsReporter(api=self.task.api)
    self.task.report = reporter.get_ad_asset_report(
      start_date=self.task.report_start_date,
      end_date=self.task.report_end_date,
      customer_id=self.task.customer_id
    )
    return f'Values fetched: {self.task.report.count()}'

class GoogleAdsAdGroupsCustomerReportFetcher(base.ReportFetcher[tasks.FetchGoogleAdsAdGroupsCustomerReportTask]):
  def fetch(self) -> Optional[any]:
    reporter = GoogleAdsReporter(api=self.task.api)
    self.task.report = reporter.get_ad_group_report(
      start_date=self.task.report_start_date,
      end_date=self.task.report_end_date,
      customer_id=self.task.customer_id
    )
    return f'Values fetched: {self.task.report.count()}'

class GoogleAdsCampaignsCustomerReportFetcher(base.ReportFetcher[tasks.FetchGoogleAdsCampaignsCustomerReportTask]):
  def fetch(self) -> Optional[any]:
    reporter = GoogleAdsReporter(api=self.task.api)
    self.task.report = reporter.get_campaign_performance_report(
      start_date=self.task.report_start_date,
      end_date=self.task.report_end_date,
      customer_id=self.task.customer_id
    )
    return f'Values fetched: {self.task.report.count()}'
