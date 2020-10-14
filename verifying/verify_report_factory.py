import tasks
import models

from . import base

def report_verifier(task: tasks.ReportTask, behavior: models.ReportTaskBehavior) -> base.ReportVerifier:
  if behavior.behavior_subtype is models.ReportTaskBehaviorSubType.before:
    return base.BeforeReportVerifier(task=task)
  elif behavior.behavior_subtype is models.ReportTaskBehaviorSubType.after:
    return base.AfterReportVerifier(task=task)
  else:
    return base.BaseReportVerifier(task=task)