import sqlalchemy as alchemy

from . import base
from typing import Optional

class CurrencyExchangeRatesTableModel(base.ReportTableModel):
  @property
  def table_name(self) -> str:
    return base.ReportTaskType.fetch_currency_exchange_rates.value

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('base', alchemy.Text),
      alchemy.Column('target', alchemy.Text),
      schema=self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'date'