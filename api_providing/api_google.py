import tasks

from . import base
from hazel import GoogleAdsAPI

class GoogleAdsAPIProvider(base.APIProvider[tasks.FetchGoogleAdsReportTask]):
  def provide(self):
    api = GoogleAdsAPI(
      developer_token=self.task.api_credentials['developer_token'],
      client_id=self.task.api_credentials['client_id'],
      client_secret=self.task.api_credentials['client_secret'],
      refresh_token=self.task.api_credentials['refresh_token'],
      login_customer_id=self.task.login_customer_id,
      customer_id=self.task.customer_id
    )
    self.task.api = api