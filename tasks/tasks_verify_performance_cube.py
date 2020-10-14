import models
from . import base

class VerifyPerformanceCubeUnfilteredAppleTask(base.VerifyReportTask):
  @property
  def task_type(self) -> models.ReportTaskType:
    return models.ReportTaskType.verify_performance_cube_unfiltered_apple
