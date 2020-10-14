import tasks
import sys, os
import tempfile
import atexit

from . import base
from typing import Dict, Optional
from pathlib import Path

class AppleCredentialsProvider(base.CredentialsProvider[tasks.FetchAppleReportTask]):
  @property
  def credentials_file_suffix(self) -> str:
    return '.zip'
  
  @property
  def credentials_locator_parameters(self) -> Dict[str, str]:
    return {'compress': 'zip'}
  
  @property
  def temp_credentials_path(self) -> Path:
    return Path(__file__).parent.parent / 'output' / 'temp'
  
  def write_certificate_contents(self, contents: bytes) -> Path:
    os_temp_file, temp_file_path = tempfile.mkstemp(dir=self.temp_credentials_path)
    os.close(os_temp_file)
    with open(temp_file_path, mode='wb') as temp_file:
      temp_file.write(contents)
    
    return Path(temp_file_path)
  
  def prepare_credentials_content(self, content: any) -> any:
    filtered_files = [f for f in content.keys() if not Path(f).name.startswith('.')]

    key_files = [f for f in filtered_files if Path(f).suffix == '.key']
    assert len(key_files) == 1

    pem_files = [f for f in filtered_files if Path(f).suffix == '.pem']
    assert len(pem_files) == 1

    if not self.temp_credentials_path.exists():
      self.temp_credentials_path.mkdir()
    
    key_file_path = self.write_certificate_contents(contents=content[key_files[0]])
    pem_file_path = self.write_certificate_contents(contents=content[pem_files[0]])

    def cleanup_tempfiles():
      key_file_path.unlink()
      pem_file_path.unlink()
    
    atexit.register(cleanup_tempfiles)
    return {
      'certificates': {
        'key': str(key_file_path.absolute()),
        'pem': str(pem_file_path.absolute())
      }
    }