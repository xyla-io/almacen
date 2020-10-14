import sqlalchemy as alchemy
import uuid
from . import base

class TaskHistory(base.ReportModelBase):
  subclasses = {}
  __abstract__ = True

  id = alchemy.Column(alchemy.CHAR(32), primary_key=True)
  creation_time = alchemy.Column(alchemy.DateTime, server_default=alchemy.func.now())
  modification_time = alchemy.Column(alchemy.DateTime, server_default=alchemy.func.now(), onupdate=alchemy.func.now())

  host = alchemy.Column(alchemy.VARCHAR(2046))
  status = alchemy.Column(alchemy.Enum('running', 'completed', 'canceled', 'error', 'reset'))
  message = alchemy.Column(alchemy.VARCHAR(2046))
  task_identifier = alchemy.Column(alchemy.VARCHAR(2046))
  task_name = alchemy.Column(alchemy.VARCHAR(2046))

  target_start_time = alchemy.Column(alchemy.DateTime)
  target_end_time = alchemy.Column(alchemy.DateTime)

  def __init__(self, task: any, host: str, status: str, message: str=None):
    self.id = uuid.uuid4().hex
    self.host = host
    self.status = status
    self.message = message
    self.task_identifier = task.identifier
    self.task_name = task.debug_description

    if task.report_start_date is not None and task.report_end_date is not None and task.report_start_date <= task.report_end_date:
      self.target_start_time = task.report_start_date
      self.target_end_time = task.report_end_date

def client_task_history_class(schema: str) -> any:
  subclass_name = f'{schema}TaskHistory'
  if subclass_name not in TaskHistory.subclasses:
    TaskHistory.subclasses[subclass_name] =  type(f'{schema}TaskHistory', 
      (TaskHistory,), 
      {
        '__tablename__': 'track_task_history',
        '__table_args__': {
          'schema': schema,
          'extend_existing': True,
        },
      }
    )
  return TaskHistory.subclasses[subclass_name]