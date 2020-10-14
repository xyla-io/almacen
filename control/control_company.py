import models
import tasks

from .control_report_factory import report_controller
from config import CompanyConfiguration
from tasks import ReportTask
from typing import List
from itertools import chain

class CompanyController:
  configuration: CompanyConfiguration
  task_history_class: any

  def __init__(self, configuration: CompanyConfiguration):
    self.configuration = configuration
    self.task_history_class = models.client_task_history_class(schema=configuration.metadata.schema)

  @property
  def tasks(self) -> List[ReportTask]:
    return [
      tasks.report_task(
        task_set=task_set,
        report_type=task_type,
        identifier_prefix='.'.join([
          self.configuration.identifier,
          task_set.action.value,
          task_set.target.value,
          task_set.identifier
        ])
      )
      for task_set in self.configuration.task_sets
      for task_type in task_set.task_types
    ]

  def run_tasks(self):
    for task in self.tasks:
      controller = report_controller(task=task, task_history_class=self.task_history_class)
      controller.run(retry_limit=2)

  def reset_tasks(self):
    for task in self.tasks:
      controller = report_controller(task=task, task_history_class=self.task_history_class)
      controller.reset()