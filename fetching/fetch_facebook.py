import pandas as pd
import tasks
import json
import re

from . import base
from typing import TypeVar, Generic, Dict
from datetime import datetime, timedelta
from figaro import FacebookReporter
from facebook_business.adobjects.adcreative import AdCreative

T = TypeVar(tasks.FetchFacebookReportTask)
class FacebookReportFetcher(Generic[T], base.ReportFetcher[T]):
  def campaign_is_relevant(self, campaign: any) -> bool:
    if not self.entity_is_relevant(entity=campaign):
      return False
    
    if self.task.campaign_regex_filter is None:
      return True
  
    if re.match(self.task.campaign_regex_filter, campaign['name']):
      return True

    return False

  def entity_is_relevant(self, entity: any) -> bool:
    updated_time = datetime.strptime(entity['updated_time'].split('T', 1)[0], '%Y-%m-%d')
    return entity['status'] == 'ACTIVE' or updated_time >= self.task.report_start_date - timedelta(days=1)

class FacebookCampaignsReportFetcher(FacebookReportFetcher[tasks.FetchFacebookCampaignsReportTask]):
  def fetch(self):
    reporter = FacebookReporter(api=self.task.api)
    all_campaigns = reporter.api.get_campaigns()
    self.task.campaigns = [c for c in all_campaigns if self.campaign_is_relevant(c)]
    
    report = reporter.get_campaign_insights_report(
      start_date=self.task.report_start_date,
      end_date=self.task.report_end_date,
      campaigns=self.task.campaigns,
      columns=self.task.fetch_columns
    )
    
    if report.empty:
      self.task.report = report
      return

    report_campaign_ids = report.campaign_id.unique()
    report_campaigns = [c for c in self.task.campaigns if c['id'] in report_campaign_ids]

    self.task.ad_sets = reporter.api.get_ad_sets(report_campaigns)
    self.task.report = report
    return 'Values fetched:\n{}'.format(self.task.report.count())

class FacebookAdSetsReportFetcher(FacebookReportFetcher[tasks.FetchFacebookAdSetsReportTask]):
  def fetch(self):
    reporter = FacebookReporter(api=self.task.api)
    all_campaigns = reporter.api.get_campaigns()

    self.task.campaigns = [c for c in all_campaigns if self.campaign_is_relevant(c)]
    self.task.ad_sets = [a for a in reporter.api.get_ad_sets(self.task.campaigns) if self.entity_is_relevant(entity=a)]
    report = reporter.get_ad_set_insights_report(
      start_date=self.task.report_start_date,
      end_date=self.task.report_end_date,
      ad_sets=self.task.ad_sets,
      columns=self.task.fetch_columns
    )

    self.task.report = report
    return 'Values fetched:\n{}'.format(self.task.report.count())

class FacebookAdsReportFetcher(FacebookReportFetcher[tasks.FetchFacebookAdsReportTask]):
  def fetch(self):
    def chunks(l, n):
      """Yield successive n-sized chunks from l."""
      for i in range(0, len(l), n):
          yield l[i:i + n]

    reporter = FacebookReporter(api=self.task.api)
    all_campaigns = reporter.api.get_campaigns()

    self.task.account = self.task.api.get_account(self.task.api.account_id)
    campaigns = [c for c in all_campaigns if self.campaign_is_relevant(c)]
    ad_sets = [a for a in reporter.api.get_ad_sets(campaigns) if self.entity_is_relevant(entity=a)]
    ads = [a for a in reporter.api.get_ads(ad_sets) if self.entity_is_relevant(entity=a)]

    report = reporter.get_ad_insights_report(
      start_date=self.task.report_start_date,
      end_date=self.task.report_end_date,
      ads=ads,
      columns=self.task.fetch_columns
    )

    if report.empty:
      self.task.report = report
      return

    report_campaign_ids = report.campaign_id.unique()
    self.task.campaigns = [c for c in campaigns if c['id'] in report_campaign_ids]
    report_ad_set_ids = report.adset_id.unique()
    self.task.ad_sets = [a for a in ad_sets if a['id'] in report_ad_set_ids]
    report_ad_ids = report.ad_id.unique()
    self.task.ads = [a for a in ads if a['id'] in report_ad_ids]

    if self.task.ads:
      ad_ids = [a['id'] for a in self.task.ads]
      ad_id_chunks = list(chunks(ad_ids, 800))
      ad_activity_histories = [
        self.task.api.get_ad_activity_history(
          start_date=self.task.report_start_date - timedelta(days=1), 
          end_date=datetime.utcnow(),
          object_ids=ids
        )
        for ids in ad_id_chunks
      ]
      self.task.ad_activity_history = [item for history in ad_activity_histories for item in history]
      extra_data = [json.loads(a['extra_data']) for a in self.task.ad_activity_history if a['event_type'] == 'update_ad_creative']

    creative_ids = {a['creative']['id'] for a in ads}
    creative_ids = creative_ids.union({d['old_value'][-1] for d in extra_data})
    creative_ids = creative_ids.union({d['new_value'][-1] for d in extra_data})
    self.task.ad_creatives = self.task.api.get_ad_creatives(object_ids=list(creative_ids))

    self.task.report = report
    return 'Values fetched:\n{}'.format(self.task.report.count())