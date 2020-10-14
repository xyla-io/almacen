import tasks
import models

from . import base
from . import credentials_apple
from . import credentials_currency_exchange

def credentials_provider(task: tasks.ReportTask) -> base.CredentialsProvider:
  if task.task_type is models.ReportTaskType.fetch_apple_search_ads_campaigns:
    return credentials_apple.AppleCredentialsProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_adgroups:
    return credentials_apple.AppleCredentialsProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_keywords:
    return credentials_apple.AppleCredentialsProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_creative_sets:
    return credentials_apple.AppleCredentialsProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_apple_search_ads_searchterms:
    return credentials_apple.AppleCredentialsProvider(task=task)
  elif task.task_type is models.ReportTaskType.fetch_base_currency_exchage_rates:
    return credentials_currency_exchange.CurrencyExchangeCredentialsProvider(task=task)
  else:
    return base.CredentialsProvider(task=task)