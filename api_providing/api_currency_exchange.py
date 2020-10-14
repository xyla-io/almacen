import tasks

from . import base
from jones import FixerAPI

class CurrencyExchangeAPIProvider(base.APIProvider[tasks.FetchBaseCurrencyExchangeReportTask]):
  def provide(self):
    api_key = self.task.api_credentials['api_key']
    self.task.api = FixerAPI(api_key=api_key)