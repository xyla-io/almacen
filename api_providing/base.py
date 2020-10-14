import tasks

from abc import abstractmethod
from typing import Generic, TypeVar

T = TypeVar(tasks.ReportTask)
class APIProvider(Generic[T]):
  task: T

  def __init__(self, task: T):
    self.task = task
  
  @abstractmethod
  def provide(self):
    pass