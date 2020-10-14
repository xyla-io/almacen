import pandas as pd
import tasks

from . import base
from datetime import datetime, timedelta
from lilu import TikTokReporter, context as tiktok_context
from typing import Optional

class TikTokReportFetcher(base.ReportFetcher[tasks.FetchTikTokReportTask]):
  def fetch(self):
    self.task.ad_account = self.task.api.get_advertiser_info()[0]
    self.task.adgroups = self.task.api.get_entities(granularity='adgroup')
    adgroup_ids = [a['adgroup_id'] for a in self.task.adgroups]
    deleted_adgroups = self.task.api.get_entities(
      granularity='adgroup',
      deleted_only=True
    )
    deleted_adgroups = list(filter(lambda a: a['adgroup_id'] not in adgroup_ids, deleted_adgroups))
    self.task.adgroups.extend(deleted_adgroups)

    if self.task.report_start_date > self.task.report_end_date:
      return
    
    reporter = TikTokReporter(api=self.task.api)
    df = reporter.get_performance_report(
      time_granularity='daily',
      start=self.task.report_start_date,
      end=self.task.report_end_date,
      entity_granularity=self.task.entity_level,
    )
    entity_columns = tiktok_context.EntityGranularity(self.task.entity_level).entity_columns
    df = reporter.add_entity_info(
      report=df,
      report_entity_granularity=self.task.entity_level,
      columns=entity_columns
    )

    deleted_df = reporter.get_performance_report(
      time_granularity='daily',
      start=self.task.report_start_date,
      end=self.task.report_end_date,
      entity_granularity=self.task.entity_level,
      deleted_only=True
    )
    deleted_df = reporter.add_entity_info(
      report=deleted_df,
      report_entity_granularity=self.task.entity_level,
      columns=entity_columns,
      deleted_only=True
    )
    if not deleted_df.empty and not df.empty:
      id_column = f'{self.task.entity_level}_{self.task.entity_level}_id'
      ids = list(map(str, df[id_column].unique()))
      deleted_df.drop(deleted_df.index[deleted_df[id_column].isin(ids)], inplace=True)
      df = df.append(deleted_df)
      df.reset_index(drop=True, inplace=True)
    elif not deleted_df.empty:
      df = deleted_df
    self.task.report = df
