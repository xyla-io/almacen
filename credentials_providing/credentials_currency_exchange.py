import tasks

from . import base

class CurrencyExchangeCredentialsProvider(base.CredentialsProvider[tasks.FetchBaseCurrencyExchangeReportTask]):
  @property
  def credentials_path(self) -> str:
    return f'{self.task.api_credentials_key}/{self.task.api_credentials_key}_fixer{self.credentials_file_suffix}'
