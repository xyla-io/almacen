from datetime import datetime
from typing import Optional

class TimeModel:
  shared: any
  _utc_now: Optional[datetime] = None
  start_date: Optional[datetime] = None
  end_date: Optional[datetime] = None
  backfill_target_date: Optional[datetime] = None
  
  @property
  def utc_now(self) -> datetime:
    return self._utc_now if self._utc_now is not None else datetime.utcnow()

  @utc_now.setter
  def utc_now(self, value):
    self._utc_now = value

TimeModel.shared = TimeModel()