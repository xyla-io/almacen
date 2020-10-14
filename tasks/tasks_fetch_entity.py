import models
import re

from . import base
from . import tasks_fetch_currency_exchange
from abc import abstractproperty
from typing import TypeVar, List, Optional

class ParseTagsMutationSpecifier:
  parser: Optional[str]
  channel: str
  entity: str
  id_column: str
  name_column: str

  @classmethod
  def task_parser_name(cls, task: base.ReportTask, default_name: Optional[str]=None) -> Optional[str]:
    if 'task_tag_parsers' in task.task_set.config and task.task_type.value in task.task_set.config['task_tag_parsers']:
      return task.task_set.config['task_tag_parsers'][task.task_type.value]
    if default_name is not None:
      return default_name
    match = re.match(r'^fetch_(.+)s$', task.task_type.value)
    if not match:
      return None
    parts = match.group(1).rpartition('_')
    name = '-'.join([parts[0], parts[2]])
    return name

  def __init__(self, parser: Optional[str], channel: str, entity: str, name_column: str, id_column: str):
    self.parser = parser
    self.channel = channel
    self.entity = entity
    self.id_column = id_column
    self.name_column = name_column

class LatestValueMutationSpecifier:
  identifier_column: str
  source_value_column: str
  target_value_column: str
  only_missing_values: List[any]

  def __init__(self, identifier_column: str, source_value_column: str, target_value_column: str, only_missing_values: List[any]=[]):
    self.identifier_column = identifier_column
    self.source_value_column = source_value_column
    self.target_value_column = target_value_column
    self.only_missing_values = only_missing_values

class FetchSpendableReportTask(base.FetchReportTask):
  @abstractproperty
  def fetched_currency_column(self) -> str:
    pass

  @abstractproperty
  def money_columns(self) -> List[str]:
    pass

  @property
  def currency(self) -> str:
    return self.task_set.company_metadata.currency

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate,
        behavior_subtype=models.ReportTaskBehaviorSubType.mutate_currency_exchange
      ),
    ]
  
  def generate_subtasks(self) -> List[TypeVar('ReportTask')]:
    if 'currency_exchange' not in self.task_set.config:
      return []
    return [
      tasks_fetch_currency_exchange.FetchCurrencyExchangeReportTask(
        task_set=self.task_set,
        identifier_prefix=self.identifier
      )
    ]

class FetchCampaignReportTask(FetchSpendableReportTask):
  @abstractproperty
  def latest_value_mutation_specifiers(self) -> List[LatestValueMutationSpecifier]:
    pass

  @property
  def parse_tags_mutation_specifier(self) -> Optional[ParseTagsMutationSpecifier]:
    return None

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.run_subtasks),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_date),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.provide_credentials),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.provide_api),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.before
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_report),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.process),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.process,
        behavior_subtype=models.ReportTaskBehaviorSubType.edit
      ),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate, 
        behavior_subtype=models.ReportTaskBehaviorSubType.replace
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.collect),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate,
        behavior_subtype=models.ReportTaskBehaviorSubType.mutate_currency_exchange
      ),
      *[
        models.ReportTaskBehavior(
          behavior_type=models.ReportTaskBehaviorType.mutate,
          behavior_subtype=models.ReportTaskBehaviorSubType.mutate_latest_column_value,
          behavior_subtype_info=s
        )
        for s in self.latest_value_mutation_specifiers
      ],
      *([models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate,
        behavior_subtype=models.ReportTaskBehaviorSubType.mutate_parse_tags,
        behavior_subtype_info=self.parse_tags_mutation_specifier
      )] if self.parse_tags_mutation_specifier else []),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]

class FetchAdReportTask(FetchCampaignReportTask):
  @abstractproperty
  def ad_name_column(self) -> str:
    pass
  
  @abstractproperty
  def ad_id_column(self) -> str:
    pass

  def generate_behaviors(self) -> List[models.ReportTaskBehavior]:
    return [
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.run_subtasks),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_date),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.provide_credentials),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.provide_api),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.before
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.fetch_report),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.process),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.process,
        behavior_subtype=models.ReportTaskBehaviorSubType.edit
      ),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate, 
        behavior_subtype=models.ReportTaskBehaviorSubType.replace
      ),
      models.ReportTaskBehavior(models.ReportTaskBehaviorType.collect),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate,
        behavior_subtype=models.ReportTaskBehaviorSubType.mutate_currency_exchange
      ),
      *[
        models.ReportTaskBehavior(
          behavior_type=models.ReportTaskBehaviorType.mutate,
          behavior_subtype=models.ReportTaskBehaviorSubType.mutate_latest_column_value,
          behavior_subtype_info=s
        )
        for s in self.latest_value_mutation_specifiers
      ],
      *([models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.mutate,
        behavior_subtype=models.ReportTaskBehaviorSubType.mutate_parse_tags,
        behavior_subtype_info=self.parse_tags_mutation_specifier
      )] if self.parse_tags_mutation_specifier else []),
      models.ReportTaskBehavior(
        behavior_type=models.ReportTaskBehaviorType.verify,
        behavior_subtype=models.ReportTaskBehaviorSubType.after
      ),
    ]