import os
import json
import glob
import importlib

from collections import OrderedDict
from typing import List, OrderedDict as OrderedDictType, Dict
from .company_configuration import CompanyConfigurations, CompanyConfiguration
from error import ConfigurationLoadError
from pathlib import Path

def import_config(name: str):
  try:
    module = importlib.import_module('config.local_{}_config'.format(name))
  except Exception:
    module = importlib.import_module('config.{}_config'.format(name))
  return getattr(module, '{}_config'.format(name))

def load_company_configs() -> OrderedDictType[str, CompanyConfigurations]:
  configs = OrderedDict()
  config_directory = os.path.dirname(os.path.realpath(__file__))
  company_paths = glob.glob(os.path.join(config_directory, 'company_configs', '*.json'))
  for company_path in company_paths:
    with open(company_path) as f:
      try:
        file_configs = CompanyConfigurations.from_json(f.read())
      except KeyboardInterrupt:
        raise
      except Exception as e:
        raise ConfigurationLoadError(file=company_path, error=e)
    file_key = os.path.splitext(os.path.basename(company_path))[0]
    configs[file_key] = file_configs
  return configs

def company_config_values(company_configs: OrderedDictType[str, CompanyConfigurations]) -> Dict[str, List[str]]:
  return {
    'files': list(sorted(company_configs.keys())),
    'companies': list(sorted({v for c in company_configs.values() for v in c.company_identifiers})),
    'products': list(sorted({p for c in company_configs.values() for p in c.product_identifiers})),
    'actions': list(sorted({a for c in company_configs.values() for a in c.action_identifiers})),
    'targets': list(sorted({t for c in company_configs.values() for t in c.target_identifiers})),
    'task_sets': list(sorted({t for c in company_configs.values() for t in c.task_set_identifiers})),
    'task_types': list(sorted({t for c in company_configs.values() for t in c.task_type_identifiers})),
  }

def load_configure() -> Dict[str, any]:
  configure_path = Path(__file__).parent.parent / 'configure.json'
  configure = json.loads(configure_path.read_text())
  return configure

sql_config = import_config('sql')
company_configs = load_company_configs()
configure = load_configure()