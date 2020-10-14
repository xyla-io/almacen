import tasks
import sqlalchemy as alchemy

from . import base
from typing import Dict, Optional

class AppleReportCollector(base.ReportCollector[tasks.FetchAppleReportTask]):
  pass

class AppleKeywordsReportCollector(AppleReportCollector):
  @property
  def column_type_transform_dictionary(self) -> Optional[Dict[str, any]]:
    return {
      'adGroupId': int,
    }