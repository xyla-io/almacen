import tasks
import pandas as pd

from . import base
from typing import TypeVar, Generic, Dict
from hashlib import sha1

T = TypeVar(tasks.FetchAppleReportTask)
class AppleReportProcessor(Generic[T], base.ReportProcessor[T]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      **super().added_columns,
      'org_id': self.task.org_id,
      'org_name': self.task.org_name,
      'converted_currency': self.task.currency,
      'product': None,
      'product_id': None,
      'product_name': None,
      'product_platform': None,
      'product_os': None,
    }

  def process(self):
    super().process()
    report = self.task.report

    if 'orgId' in report:
      assert len(report.orgId.unique()) == 1 and report.orgId.unique()[0] == self.task.org_id
      report.drop(columns='orgId', inplace=True) # we already store this off using our 'org_id' column

    # map v2 column names to v1 column names
    report.rename(
      columns={
        'installs': 'conversions',
        'latOnInstalls': 'conversionsLATon',
        'latOffInstalls': 'conversionsLAToff',
        'newDownloads': 'conversionsNewDownloads',
        'redownloads': 'conversionsRedownloads'
      },
      inplace=True
    )

    report['app_display_name'] = report.adamId.map(self.task.app_id_display_names)
    if not self.task.keep_empty_app_display_names:
      report.drop(report.index[report.app_display_name.isnull()], inplace=True)

    if 'countriesOrRegions' in report:
      report.drop(columns='countriesOrRegions', inplace=True)

    if 'countryOrRegionServingStateReasons' in report:
      report.drop(columns='countryOrRegionServingStateReasons', inplace=True)

    report['platform'] = 'ios'
    report['product_id'] = report.adamId.apply(lambda i: str(i) if not pd.isna(i) else None)
    self.add_product_canonical_columns(report=report)

class AppleCreativeSetsReportProcessor(AppleReportProcessor):
  def process(self):
    super().process()
    if self.task.report.empty:
      return
    self.task.report.adGroupCreativeSetId = self.task.report.adGroupCreativeSetId.astype(pd.Int64Dtype())

class AppleKeywordsReportProcessor(AppleReportProcessor):
  def process(self):
    super().process()
    if self.task.report.empty:
      return
    self.task.report['ad_id'] = self.task.report.apply(
      lambda r: f'flx-kw-{int(r.campaignId)}-{int(r.adGroupId)}-' + (sha1(r.keyword.encode('utf-8')).hexdigest() if r.keyword else ''),
      axis=1
    )

class AppleSearchTermsReportProcessor(AppleKeywordsReportProcessor):
  def process(self):
    super().process()
    if self.task.report.empty:
      return
    self.task.report['searchterm_id'] = self.task.report.apply(
      lambda r: f'flx-st-{int(r.campaignId)}-{int(r.adGroupId)}-' + (f'{int(r.keywordId)}-' if not pd.isna(r.keywordId) else '-') + (sha1(r.searchTermText.encode('utf-8')).hexdigest() if r.searchTermText else ''),
      axis=1
    )