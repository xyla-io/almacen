import tasks
import sys, os

from . import base
from azrael import SnapchatAPI

class SnapchatAPIProvider(base.APIProvider[tasks.FetchSnapchatReportTask]):
  def provide(self):
    api = SnapchatAPI(
      refresh_token=self.task.api_credentials['refresh_token'],
      client_id=self.task.api_credentials['client_id'],
      client_secret=self.task.api_credentials['client_secret']
    )
    api.ad_account_id = self.task.ad_account_id
    self.task.api = api