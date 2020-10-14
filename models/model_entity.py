import sqlalchemy as alchemy

from . import base

class EntityTableModel(base.ReportTableModel):
  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      schema=self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'fetch_date'

class EntityCampaignTableModel(EntityTableModel):
  @property
  def table_name(self) -> str:
    return 'entity_campaign_materialized'

class EntityAdsetTableModel(EntityTableModel):
  @property
  def table_name(self) -> str:
    return 'entity_adset_materialized'

class EntityAdTableModel(EntityTableModel):
  @property
  def table_name(self) -> str:
    return 'entity_ad_materialized'