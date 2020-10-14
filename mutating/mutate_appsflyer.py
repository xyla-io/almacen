import tasks
import models

from . import base
from .mutate_query_base import UpdateQuery
from data_layer import SQLQuery
from datetime import datetime, timedelta
from typing import Optional, List, OrderedDict as OrderedDictType
from collections import OrderedDict

class AppsFlyerDataLockerCrystallizationQuery(UpdateQuery):
  @property
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'event_date_crystallized = event_date > (select min(event_date) from {self.full_target_name}) and event_date < (select max(event_date) from {self.full_target_name})'),
      SQLQuery(f'effective_date_crystallized = effective_date > (select min(event_date) from {self.full_target_name}) and effective_date < (select max(event_date) from {self.full_target_name})'),
    ]

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery('crystallized'),
      SQLQuery('(not event_date_crystallized or not effective_date_crystallized)'),
    ]

class AppsFlyerDataLockerCrystallizationMutator(base.QueryReportMutator[tasks.FetchAppsFlyerDataLockerReportTask]):
  @property
  def query(self) -> SQLQuery:
    return AppsFlyerDataLockerCrystallizationQuery(full_target_name=self.task.report_table_model.full_table_name)