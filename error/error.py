from typing import List

class AlmacenError(Exception):
  pass

class ConfigurationLoadError(AlmacenError):
  def __init__(self, file: str, error: Exception):
    super().__init__(f'Error loading configuration from {file}: {error}')

class VerificationError(AlmacenError):
  def __init__(self, verifications: List[str]):
    super().__init__(f'{len(verifications)} verification{"s" if len(verifications) != 1 else ""} failed: {",".join(verifications)}')