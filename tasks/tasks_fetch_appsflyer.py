import models

from . import base
from typing import List, Dict, Optional
from salem import AppsFlyerAPI
from datetime import timedelta

class FetchAppsFlyerReportTask(base.FetchReportTask):
  api: Optional[AppsFlyerAPI]

  @property
  def debug_description(self) -> str:
    return '{}: ({}) — {}'.format(
      self.company_display_name,
      self.app_id,
      self.task_type.value
    )

  @property
  def app_id(self) -> str:
    assert len(self.task_set.config['platforms']) == 1
    assert len(self.task_set.products) == 1

    platform = self.task_set.config['platforms'][0]
    product = self.task_set.products[0]
    id = product.platform_ids[platform]
    return f'id{id}' if platform == 'ios' else id

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'app_id': self.app_id,
    }

  @property
  def crystallization_time(self) -> timedelta:
    return timedelta(days=1, hours=6)
  
  @property
  def default_fetch_columns(self) -> List[str]:
    return [
      'AppsFlyer ID',
      'Attributed Touch Type',
      'Attributed Touch Time',
      'Install Time',
      'Is Retargeting',
      'Is Primary Attribution',
      'Event Time',
      'Event Name',
      'Event Value',
      'Event Revenue',
      'Event Revenue Currency',
      'Event Revenue USD',
      'Media Source',
      'Channel',
      'Keywords',
      'Campaign',
      'Campaign ID',
      'Adset',
      'Adset ID',
      'Ad',
      'Ad ID',
      'Ad Type',
      'Country Code',
      'State',
      'City',
      'Postal Code',
      'DMA',
      'IP',
      'IDFA',
      'Advertising ID',
      'Android ID',
      'IDFV',
      'Platform',
      'Device Type',
      'OS Version',
      'App Version',
      'App Name',
    ]

class FetchAppsFlyerPurchaseEventsReportTask(FetchAppsFlyerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_appsflyer_purchase_events

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppsFlyerPurchaseEventsReportTableModel(schema_name=self.report_table_schema)

class FetchAppsFlyerInstallEventsReportTask(FetchAppsFlyerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_appsflyer_install_events

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppsFlyerInstallEventsReportTableModel(schema_name=self.report_table_schema)

class FetchAppsFlyerCustomEventsReportTask(FetchAppsFlyerReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_appsflyer_custom_events

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.AppsFlyerCustomEventsReportTableModel(schema_name=self.report_table_schema)

  @property
  def custom_event_names(self) -> List[str]:
    return self.task_set.config['custom_event_names']

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'app_id': self.app_id,
      'event name': self.custom_event_names,
    }