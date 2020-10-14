import sys
import models
import tasks
import credentials_providing
import api_providing
import fetching
import processing
import collecting
import mutating
import verifying
import tracking
import functools
import logging

from typing import TypeVar, Generic, Optional, List
from abc import abstractmethod
from textwrap import indent
from enum import Enum

def log(func):
  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    value = func(*args, **kwargs)
    logging.info(value)
    return value
  return wrapper

class Color(Enum):
  red = '\033[91m'
  green = '\033[92m'
  blue = '\033[94m'
  yellow = '\033[93m'

  def colored(self, text: str):
    return '{}{}\033[0m'.format(self.value, text)

T = TypeVar(tasks.ReportTask)
class ReportController(Generic[T]):
  task: T
  tracker: tracking.ReportTaskTracker
  sublevel: int

  def __init__(self, task: T, task_history_class: any, sublevel: int=0):
    self.task = task
    self.tracker = tracking.ReportTaskTracker(task=task, task_history_class=task_history_class)
    self.sublevel = sublevel

  def reset(self):
    self.log('\nResetting: ' + self.task.debug_description, color=Color.yellow)
    self.tracker.track_task_reset()

  def run(self, retry_limit=0, previous_retries: List[str]=[]):
    try:
      self.retrieve_last_run_history()
      self.run_task()
      self.tracker.track_task_completion()
    except Exception as e:
      self.tracker.track_task_error(error=e)
      if self.task.retry is not None and self.task.retry not in previous_retries and len(previous_retries) < retry_limit:
        self.log(f'\nRetrying ({self.task.retry}): {self.task.debug_description}', color=Color.red)
        self.run(retry_limit=retry_limit, previous_retries=previous_retries + [self.task.retry])
      else:
        raise e
    finally:
      self.task.sql_layer = None
  
  def retrieve_last_run_history(self):
    self.task.last_run_history = self.tracker.get_last_tracked_history()

  def run_task(self):
    self.log('\nRunning: ' + self.task.debug_description, color=Color.green)
    
    for behavior in self.task.behaviors:
      self.log(f'--> {behavior.behavior_type.value}')

      if behavior.behavior_type is models.ReportTaskBehaviorType.fetch_date:
        date_fetcher = fetching.report_date_fetcher(task=self.task, behavior=behavior)
        behavior.behavior_result = date_fetcher.fetch()
        self.log(f'• Start: {self.task.report_start_date}', sublevel=1, color=Color.blue)
        self.log(f'• End: {self.task.report_end_date}', sublevel=1, color=Color.blue)

      elif behavior.behavior_type is models.ReportTaskBehaviorType.provide_credentials:
        credentials_provider = credentials_providing.credentials_provider(task=self.task)
        behavior.behavior_result = credentials_provider.provide()

      elif behavior.behavior_type is models.ReportTaskBehaviorType.provide_api:    
        api_provider = api_providing.api_provider(task=self.task, behavior=behavior)
        behavior.behavior_result = api_provider.provide()

      elif behavior.behavior_type is models.ReportTaskBehaviorType.fetch_report:    
        report_fetcher = fetching.report_fetcher(task=self.task, behavior=behavior)
        behavior.behavior_result = report_fetcher.fetch()
        self.log(
          '--> report: {}\nRowcount: {}'.format(
            self.task.report.head() if self.task.report is not None else None, 
            len(self.task.report) if self.task.report is not None else '0'
          ),
          sublevel=1
        )

      elif behavior.behavior_type is models.ReportTaskBehaviorType.process:
        report_processor = processing.report_processor(task=self.task, behavior=behavior)
        behavior.behavior_result = report_processor.process()

      elif behavior.behavior_type is models.ReportTaskBehaviorType.collect:
        report_collector = collecting.report_collector(task=self.task, behavior=behavior)
        behavior.behavior_result = report_collector.collect()

      elif behavior.behavior_type is models.ReportTaskBehaviorType.mutate:
        report_mutator = mutating.report_mutator(task=self.task, behavior=behavior)
        behavior.behavior_result = report_mutator.mutate()

      elif behavior.behavior_type is models.ReportTaskBehaviorType.verify:
        report_verifier = verifying.report_verifier(task=self.task, behavior=behavior)
        behavior.behavior_result = report_verifier.verify()

      elif behavior.behavior_type is models.ReportTaskBehaviorType.run_subtasks:
        self.log('\nRunning subtasks...', color=Color.yellow)
        self.task.subtasks = self.task.generate_subtasks()
        subtask_controllers = [ReportController(task=t, task_history_class=self.tracker.task_history_class, sublevel=self.sublevel + 1) for t in self.task.subtasks]
        subtask_count = len(subtask_controllers)
        result = f'Ran {subtask_count} subtasks:'
        for index, subtask_controller in enumerate(subtask_controllers):
          self.log(f'--> subtask {index + 1} of {subtask_count}', sublevel=1)
          subtask_controller.run()
          result += f'\n  --> {subtask_controller.task.debug_description}'
        behavior.behavior_result = result

      else:
        raise ValueError('Unknown task behavior type', behavior.behavior_type)
      
      if behavior.behavior_result is not None:
        self.log(behavior.behavior_result, color=Color.yellow)
    
    self.log(f'\nCompleted: {self.task.debug_description}', color=Color.green)
    self.log(f'• Start: {self.task.report_start_date}', sublevel=1, color=Color.blue)
    self.log(f'• End: {self.task.report_end_date}\n', sublevel=1, color=Color.blue)

  @log
  def get_log_text(self, message: str, sublevel: int=0, color: Optional[Color]=None) -> str:
    text = message if color is None else color.colored(message)
    prefix = f'{self.task.task_set.company_metadata.schema} ::: ' + '  ' * (self.sublevel + sublevel)
    return indent(text, prefix)

  def log(self, message: str, sublevel: int=0, color: Optional[Color]=None):
    text = self.get_log_text(message=message, sublevel=sublevel, color=color)
    print(text)
    sys.stdout.flush()

class BaseReportController(ReportController[tasks.ReportTask]):
  pass