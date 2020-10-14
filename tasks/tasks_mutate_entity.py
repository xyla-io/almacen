import models
from . import base

class MaterializeEntityTask(base.MutateReportTask):
  @property
  def debug_description(self) -> str:
    return '{} — {}'.format(
      self.company_display_name,
      self.task_type.value,
    )

  @property
  def materialize_derived_entities(self) -> bool:
    return self.task_set.config['materialize_derived_entities'] if 'materialize_derived_entities' in self.task_set.config else False

class MaterializeEntityCampaignTask(MaterializeEntityTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_entity_campaign

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.EntityCampaignTableModel(schema_name=self.report_table_schema)

class MaterializeEntityAdsetTask(MaterializeEntityTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_entity_adset

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.EntityAdsetTableModel(schema_name=self.report_table_schema)

class MaterializeEntityAdTask(MaterializeEntityTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.materialize_entity_ad

  @property
  def report_table_model(self) -> models.ReportTableModel:
    return models.EntityAdTableModel(schema_name=self.report_table_schema)