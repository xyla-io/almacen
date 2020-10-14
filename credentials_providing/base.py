import tasks
import json
import urllib

from credentials import access_credentials
from data_layer import locator_factory, ResourceLocator
from typing import Generic, TypeVar, Dict, Optional

T = TypeVar(tasks.FetchReportTask)
class CredentialsProvider(Generic[T]):
  task: T

  def __init__(self, task: T):
    self.task = task
  
  @property
  def credentials_file_suffix(self) -> str:
    return '.json'
  
  @property
  def credentials_locator_parameters(self) -> Dict[str, str]:
    return {'encoding': 'utf-8'}

  @property
  def credentials_path(self) -> str:
    return f'{self.task.api_credentials_key}/{self.task.api_credentials_key}_{self.task.task_set.target.value}{self.credentials_file_suffix}'

  @property
  def credentials_url(self) -> str:
    parts = urllib.parse.urlparse(access_credentials['credentials_url'])
    url = urllib.parse.urlunparse([
      parts.scheme,
      parts.netloc,
      f'{parts.path}{self.credentials_path}',
      parts.params,
      parts.query,
      parts.fragment
    ])

    url, base_params = ResourceLocator.strip_locator_parameters(url=url)
    return ResourceLocator.append_locator_parameters(
      url=url,
      parameters={
        **base_params,
        **self.credentials_locator_parameters
      }
    )

  def get_credentials_content(self) -> any:
    locator = locator_factory(url=self.credentials_url)
    resource = locator.get()
    return resource
  
  def prepare_credentials_content(self, content: any) -> Dict[str, any]:
    return json.loads(content)

  def get_credentials(self) -> any:
    content = self.get_credentials_content()
    return self.prepare_credentials_content(content)

  def provide(self):
    self.task.api_credentials = self.get_credentials()