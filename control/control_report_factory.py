import tasks
import models

from . import base

def report_controller(task: tasks.ReportTask, task_history_class: any) -> base.ReportController:
  return base.BaseReportController(task=task, task_history_class=task_history_class)
