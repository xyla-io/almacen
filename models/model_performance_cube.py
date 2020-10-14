import sqlalchemy as alchemy

from . import base
from enum import Enum

class PerformanceCubeCohortAnchor(Enum):
  attribution = 'attribution'
  install = 'install'

class PerformanceCubeTimeline(Enum):
  cohort = 'cohort'
  time_series = 'time_series'

class PerformanceCubeAdAlias(Enum):
  keyword = 'keyword'

class PerformanceCubeUnfilteredTableModel(base.ReportTableModel):
  @property
  def table_name(self) -> str:
    return 'performance_cube_unfiltered'

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('source', alchemy.Text),
      schema=self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'effective_date'

class PerformanceCubeFilteredTableModel(base.ReportTableModel):
  @property
  def table_name(self) -> str:
    return 'performance_cube_filtered'

  def define_table(self):
    alchemy.Table(
      self.table_name,
      self.declarative_base.metadata,
      alchemy.Column(self.date_column_name, alchemy.Date),
      alchemy.Column(self.crystallized_column_name, alchemy.Boolean),
      alchemy.Column('source', alchemy.Text),
      schema=self.schema_name
    )

  @property
  def date_column_name(self) -> str:
    return 'effective_date'