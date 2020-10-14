import socket
import sqlalchemy as alchemy
import tasks
import models

from .track_query import ResetTaskHistoryQuery
from typing import Optional

class ReportTaskTracker:
  task: tasks.ReportTask
  host: str
  task_history_class: any

  def __init__(self, task: tasks.ReportTask, task_history_class: any):
    self.task = task
    self.host = socket.gethostname()
    self.task_history_class = task_history_class

  def get_last_tracked_history(self) -> Optional[models.TaskHistory]:
    session = self.task.sql_layer.alchemy_session()
    return session.query(self.task_history_class) \
            .filter(self.task_history_class.task_identifier == self.task.identifier) \
            .order_by(self.task_history_class.modification_time.desc()) \
            .limit(1) \
            .one_or_none()

  def track_task_completion(self):
    history = self.task_history_class(
      task=self.task,
      host=self.host,
      status='completed'
    )
    self.add_history(history)

  def track_task_reset(self):
    query = ResetTaskHistoryQuery(
      schema_prefix=self.task_history_class.__table_args__['schema'] + '.',
      task_identifier=self.task.identifier,
      host=self.host
    )
    self.task.sql_layer.connect()
    query.run(sql_layer=self.task.sql_layer)
    self.task.sql_layer.commit()
    self.task.sql_layer.disconnect()

  def track_task_error(self, error: Exception):
    history = self.task_history_class(
      task=self.task,
      host=self.host,
      status='error',
      message=repr(error)[:2046]
    )
    self.add_history(history)

  def add_history(self, history: models.TaskHistory):
    session = self.task.sql_layer.alchemy_session()
    session.add(history)
    session.commit()