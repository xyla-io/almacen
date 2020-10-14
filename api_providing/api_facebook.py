import tasks

from . import base
from figaro import FacebookAPI

class FacebookAPIProvider(base.APIProvider[tasks.FetchFacebookReportTask]):
  def provide(self):
    api = FacebookAPI(
      app_id=self.task.api_credentials['app_id'],
      app_secret=self.task.api_credentials['app_secret'],
      access_token=self.task.api_credentials['access_token'],
      ad_user_id=self.task.api_credentials['ad_user_id'],
      api_version='v7.0'
    )
    api.account_id = self.task.account_id
    self.task.api = api