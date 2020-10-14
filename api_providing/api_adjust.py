import tasks

from . import base
from garfield import AdjustAPI

class AdjustAPIProvider(base.APIProvider[tasks.FetchAdjustReportTask]):
  def provide(self):
    user_token = self.task.api_credentials['user_token']
    self.task.api = AdjustAPI(user_token=user_token, app_token=self.task.app_token)