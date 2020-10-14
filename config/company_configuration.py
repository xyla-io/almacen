from __future__ import annotations

import json

from models import ReportAction, ReportTarget, ReportTaskType
from typing import Dict, List, Optional, Union

class CompanyConfiguration:
  class Metadata:
    schema: str
    display_name: str
    currency: str

    def __init__(self, configuration: Dict[str, any]):
      self.schema = configuration['schema']
      self.display_name = configuration['display_name']
      self.currency = configuration['currency']
    
    @property
    def as_dict(self) -> Dict[str, any]:
      return {
        'schema': self.schema,
        'display_name': self.display_name,
        'currency': self.currency
      }
  
  class Product:
    identifier: str
    display_name: str
    platform_ids: Dict[str, any]

    def __init__(self, identifier: str, configuration: Dict[str, any]):
      self.identifier = identifier
      self.display_name = configuration['display_name']
      self.platform_ids = configuration['platform_ids']
    
    @property
    def as_dict(self) -> Dict[str, any]:
      return {
        'display_name': self.display_name,
        'platform_ids': self.platform_ids,
      }
  
  class TaskSet:
    identifier: str
    config: Dict[str, any]
    action: ReportAction
    target: ReportTarget
    products: List[CompanyConfiguration.Product]
    credentials_key: Optional[str]
    company_metadata: CompanyConfiguration.Metadata
    task_types: List[ReportTaskType]

    def __init__(self, identifier: str, configuration: Dict[str, any], products_context: Dict[str, CompanyConfiguration.Product], company_metadata: CompanyConfiguration.Metadata):
      self.identifier = identifier
      self.config = configuration
      self.action = ReportAction(configuration['action'])
      self.target = ReportTarget(configuration['target'])
      self.products = [
        products_context[i] for i in configuration['product_identifiers']
      ] if 'product_identifiers' in configuration else []
      self.credentials_key = configuration['credentials_key'] if 'credentials_key' in configuration else None
      self.company_metadata = company_metadata
      self.task_types = [ReportTaskType(t) for t in configuration['task_types']]

    @property
    def as_dict(self) -> Dict[str, any]:
      return {
        **self.config,
        'task_types': [t.value for t in self.task_types],
        'product_identifiers': [p.identifier for p in self.products],
        'credentials_key': self.credentials_key if self.credentials_key else None,
      }

    @property
    def task_type_identifiers(self) -> List[str]:
      return list(sorted({t.value for t in self.task_types}))

    def filtered(self, task_types_filter: Optional[List[ReportTaskType]]=None, products_filter: Optional[List[str]]=None) -> CompanyConfiguration.TaskSet:
      filtered = CompanyConfiguration.TaskSet(
        identifier=self.identifier,
        configuration=self.as_dict,
        products_context={p.identifier: p for p in self.products},
        company_metadata=self.company_metadata
      )
      if task_types_filter is not None:
        filtered.task_types = [t for t in filtered.task_types if t in task_types_filter]
      if products_filter is not None:
        filtered.products = [p for p in filtered.products if p.identifier in products_filter]
      return filtered

    def product_for_platform_id(self, platform_id: str) -> Optional[CompanyConfiguration.Product]:
      products = {p for p in self.products if platform_id in {str(i) for i in p.platform_ids.values()}}
      if len(products) > 1:
        raise ValueError(f'Multiple products matched by platform ID {platform_id}', products)
      return next(iter(products)) if products else None

  # ----------------------------
  # class CompanyConfiguration:
  # ----------------------------
  identifier: str
  metadata: CompanyConfiguration.Metadata
  _products: Dict[str, Product]
  _task_sets: Dict[str, TaskSet]

  @classmethod
  def from_legacy(cls, identifier, legacy_configuration: Dict[str, any]) -> CompanyConfiguration:
    configuration = {
      'company_metadata': legacy_configuration['company_metadata'],
      'products': legacy_configuration['products'],
      'task_sets': {},
    }
    action_configs = legacy_configuration['actions']
    for action_key, action_configs in action_configs.items():
      for target, target_configs in action_configs['targets'].items():
        for task_set_identifier, task_set_config in target_configs['task_sets'].items():
          configuration['task_sets'][f'{action_key}_{target}_{task_set_identifier}'] = {
            **task_set_config,
            'identifier': f'{action_key}_{target}_{task_set_identifier}',
            'action': action_key,
            'target': target,
          }
    return cls(identifier=identifier, configuration=configuration)

  def __init__(self, identifier: str, configuration: Dict[str, any]):
    self.identifier = identifier
    company_metadata = {
      'schema': self.identifier,
      **configuration['company_metadata'],
    }
    self.metadata = CompanyConfiguration.Metadata(configuration=company_metadata)

    products_config = configuration['products']
    self._products = {
      i: CompanyConfiguration.Product(identifier=i, configuration=products_config[i])
      for i in products_config.keys()
    }
    
    self._task_sets = {
      task_set_identifier: CompanyConfiguration.TaskSet(
        identifier=task_set_identifier,
        configuration=task_set_config,
        products_context=self._products,
        company_metadata=self.metadata
      ) for task_set_identifier, task_set_config in configuration['task_sets'].items()
    }

  @property
  def products(self) -> List[CompanyConfiguration.Product]:
    return [self._products[i] for i in sorted(self._products.keys())]

  @property
  def task_sets(self) -> List[CompanyConfiguration.TaskSet]:
    return [self._task_sets[i] for i in sorted(self._task_sets.keys())]

  @property
  def product_identifiers(self) -> List[str]:
    return [p.identifier for p in self.products]

  @property
  def task_set_identifiers(self) -> List[str]:
    return [t.identifier for t in self.task_sets]

  @property
  def action_identifiers(self) -> List[str]:
    return list(sorted({t.action.value for t in self.task_sets}))

  @property
  def target_identifiers(self) -> List[str]:
    return list(sorted({t.target.value for t in self.task_sets}))

  @property
  def task_type_identifiers(self) -> List[str]:
    return list(sorted({v for t in self.task_sets for v in t.task_type_identifiers}))

  @property
  def as_dict(self) -> Dict[str, any]:
    return {
      'company_metadata': self.metadata.as_dict,
      'products': {p.identifier: p.as_dict for p in self.products},
      'task_sets': {t.identifier: t.as_dict for t in self.task_sets}
    }

  def filtered(self, actions_filter: Optional[List[ReportAction]]=None, targets_filter: Optional[List[ReportTarget]]=None, task_sets_filter: Optional[List[str]]=None, task_types_filter: Optional[List[ReportTaskType]]=None) -> CompanyConfiguration:
    filtered = CompanyConfiguration(identifier=self.identifier, configuration=self.as_dict)

    if actions_filter is not None:
      filtered._task_sets = {s.identifier: s for s in filtered.task_sets if s.action in actions_filter}
    if targets_filter is not None:
      filtered._task_sets = {s.identifier: s for s in filtered.task_sets if s.target in targets_filter}
    if task_sets_filter is not None:
      filtered._task_sets = {s.identifier: s for s in filtered.task_sets if s.identifier in task_sets_filter}

    filtered._task_sets = {s.identifier: s.filtered(task_types_filter=task_types_filter) for s in filtered.task_sets}
    filtered._task_sets = {i: s for i, s in filtered._task_sets.items() if s.task_types}
    return filtered

class CompanyConfigurations:
  _configs: Dict[str, CompanyConfiguration]

  def __init__(self, configuration: Dict[str, any]):
    if 'legacy' in configuration and configuration['legacy']:
      del configuration['legacy']
      self._configs = {i: CompanyConfiguration.from_legacy(identifier=i, legacy_configuration=configuration[i]) for i in configuration.keys()}
    else:
      self._configs = {i: CompanyConfiguration(identifier=i, configuration=configuration[i]) for i in configuration.keys()}

  @classmethod
  def for_schema(cls, schema: str) -> CompanyConfigurations:
    config = {
      schema: {
        'company_metadata': {
          'currency': '',
          'display_name': '',
        },
        'products': {},
        'task_sets': {},
      }
    }
    return cls(configuration=config)

  @classmethod
  def from_legacy_json(cls, legacy_json: str) -> CompanyConfigurations:
    return cls(configuration={'legacy': True, **json.loads(legacy_json)})

  @classmethod
  def from_json(cls, data: str):
    data = json.loads(data)
    return CompanyConfigurations(configuration=data)

  @classmethod
  def from_sql(cls, schema: str):
    pass

  @property
  def configs(self) -> List[CompanyConfiguration]:
    return [self._configs[i] for i in sorted(self._configs.keys())]

  @property
  def company_identifiers(self) -> List[str]:
    return [c.identifier for c in self.configs]

  @property
  def product_identifiers(self) -> List[str]:
    return list(sorted({p for c in self.configs for p in c.product_identifiers}))

  @property
  def task_set_identifiers(self) -> List[str]:
    return list(sorted({t for c in self.configs for t in c.task_set_identifiers}))

  @property
  def action_identifiers(self) -> List[str]:
    return list(sorted({a for c in self.configs for a in c.action_identifiers}))

  @property
  def target_identifiers(self) -> List[str]:
    return list(sorted({t for c in self.configs for t in c.target_identifiers}))

  @property
  def task_type_identifiers(self) -> List[str]:
    return list(sorted({t for c in self.configs for t in c.task_type_identifiers}))
  
  def filtered(self, companies_filter: Optional[List[str]]=None, actions_filter: Optional[List[ReportAction]]=None, targets_filter: Optional[List[ReportTarget]]=None, task_sets_filter: Optional[List[str]]=None, task_types_filter: Optional[List[ReportTaskType]]=None) -> CompanyConfigurations:
    filtered = CompanyConfigurations(configuration=self.as_dict)
    if companies_filter is not None:
      filtered._configs = {c.identifier: c for c in filtered.configs if c.identifier in companies_filter}
    filtered._configs = {
      c.identifier: c.filtered(
        actions_filter=actions_filter,
        targets_filter=targets_filter,
        task_sets_filter=task_sets_filter,
        task_types_filter=task_types_filter
      )
      for c in filtered.configs
    }
    return filtered

  def as_sql(self, schema: str):
    pass

  @property
  def as_json(self) -> str:
    return json.dumps(self.as_dict, sort_keys=True, indent=2)

  @property
  def as_dict(self) -> Dict[str, any]:
    return { c.identifier: c.as_dict for c in self.configs }