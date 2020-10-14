import tasks
import pandas as pd

from typing import TypeVar, Generic, Dict, List, Tuple, Optional
from functools import reduce

T = TypeVar(tasks.FetchReportTask)
class ReportFetcher(Generic[T]):
  task: T

  def __init__(self, task: T):
    self.task = task

  def fetch(self):
    raise NotImplementedError()

class VoidReportFetcher(ReportFetcher[tasks.FetchReportTask]):
  def fetch(self):
    pass

cache: List[Tuple[Dict[str, any], pd.DataFrame, Dict[str, any]]] = []

def set_cache(key: Dict[str, any], report: pd.DataFrame, context: Dict[str, any]={}):
  global cache
  cache = list(filter(lambda t: t[0] != key, cache))
  cache.append((key, report, {**context}))
  while reduce(lambda v, t: v + t[1].memory_usage.sum(), cache, 0) > 1000000:
    cache = cache[1:]

def get_cache(key: Dict[str, any]) -> Optional[Tuple[pd.DataFrame, Dict[str, any]]]:
  for item in cache:
    if item[0] == key:
      return item[1:]
  return None

def get_all_cache() -> List[Tuple[Dict[str, any], pd.DataFrame, Dict[str, any]]]:
  return [*cache]