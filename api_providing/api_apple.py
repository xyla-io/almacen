import tasks
import sys, os

from . import base
from heathcliff import SearchAdsAPI

class AppleAPIProvider(base.APIProvider[tasks.FetchAppleReportTask]):
  def provide(self):
    certificates = self.task.api_credentials['certificates']
    self.task.api = SearchAdsAPI(certificates=certificates, org_id=self.task.org_id)