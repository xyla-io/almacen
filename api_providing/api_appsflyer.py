import tasks

from . import base
from salem import AppsFlyerAPI, AppsFlyerDataLockerAPI

class AppsFlyerAPIProvider(base.APIProvider[tasks.FetchAppsFlyerReportTask]):
  def provide(self):
    api_key = self.task.api_credentials['api_key']
    self.task.api = AppsFlyerAPI(api_key=api_key, app_id=self.task.app_id)

class AppsFlyerDataLockerAPIProvider(base.APIProvider[tasks.FetchAppsFlyerDataLockerReportTask]):
  def provide(self):
    credentials = self.task.api_credentials
    self.task.api = AppsFlyerDataLockerAPI(
      aws_access_key_id=credentials['aws_access_key'],
      aws_secret_access_key=credentials['aws_secret_key'],
      region=self.task.bucket_region,
      bucket_name=self.task.bucket_name,
      hourly_data_path=self.task.hourly_data_path
    )