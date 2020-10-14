import os
import json
import importlib

def load_credentials(name: str):
  credentials_directory = os.path.dirname(os.path.realpath(__file__))
  try:
    with open(os.path.join(credentials_directory, f'local_{name}_credentials.json')) as f:
      config = json.load(f)
  except FileNotFoundError:
    with open(os.path.join(credentials_directory, f'{name}_credentials.json')) as f:
      config = json.load(f)
  return config

access_credentials = load_credentials('access')