import models

from . import base
from config import CompanyConfiguration
from typing import List, Dict, Optional
from jones import FixerAPI
from datetime import datetime

class FetchBaseCurrencyExchangeReportTask(base.FetchReportTask):
  api: Optional[FixerAPI]
  min_currency_dates_to_fetch: Dict[str, datetime]
  max_currency_dates_to_fetch: Dict[str, datetime]
  base_currency: str

  def __init__(self, base_currency: str, task_set: CompanyConfiguration.TaskSet, identifier_prefix: str):
    super().__init__(task_set=task_set, identifier_prefix=identifier_prefix)
    self.base_currency = base_currency
    self.min_currency_dates_to_fetch = {}
    self.max_currency_dates_to_fetch = {}

  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_base_currency_exchage_rates

  @property
  def debug_description(self) -> str:
    return '{}: ({} -> {}) — {}'.format(
      self.company_display_name,
      self.base_currency,
      ', '.join(self.currencies),
      self.task_type.value
    )

  @property
  def task_identifier_columns(self) -> Dict[str, any]:
    return {
      'base': self.base_currency,
      'target': self.currencies,
    }

  @property
  def currencies(self) -> List[str]:
    return self.task_set.config['currency_exchange']['currencies']
  
  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.CurrencyExchangeRatesTableModel(schema_name=self.report_table_schema)

  @property
  def api_credentials_key(self) -> str:
    return self.task_set.config['currency_exchange']['credentials_key']

class FetchCurrencyExchangeReportTask(base.CombinedReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.fetch_currency_exchange_rates

  @property
  def debug_description(self) -> str:
    return '{}: ({}) — {}'.format(
      self.company_display_name,
      ', '.join(self.currencies),
      self.task_type.value
    )

  @property
  def currencies(self) -> List[str]:
    return self.task_set.config['currency_exchange']['currencies']

  def generate_subtasks(self) -> List[base.ReportTask]:
    return [
      FetchBaseCurrencyExchangeReportTask(
        base_currency=c,
        task_set=self.task_set,
        identifier_prefix='{}.{}-{}'.format(self.identifier, c, '_'.join(self.currencies))
      )
      for c in self.currencies
    ]
