import tasks
import sys, os

from . import base
from lilu import TikTokAPI

class TikTokAPIProvider(base.APIProvider[tasks.FetchTikTokCampaignsReportTask]):
  def provide(self):
    api = TikTokAPI(
      access_token=self.task.api_credentials['access_token'],
      client_secret=self.task.api_credentials['secret'],
      app_id=self.task.api_credentials['app_id'],
      advertiser_id=self.task.advertiser_id
    )
    self.task.api = api