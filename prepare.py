import os
import sys
import preparing
import click
import models
import glob
import pandas as pd

from datetime import datetime
from config import sql_config, company_configs, company_config_values, CompanyConfigurations
from getpass import getpass
from random import randint
from pathlib import Path
from typing import Tuple, List, Dict, Optional
from preparing.prepare_sql import Status
from collections import OrderedDict

all_config_values = company_config_values(company_configs)

def run_sql_action(preparer: preparing.SQLPreparer, target: preparing.PrepareSQLTarget, drop_if_exist:bool=False, replace_if_exist: bool=True, relations:List[str]=[]):
  if target is preparing.PrepareSQLTarget.settings:
    preparer.configure_settings()

  elif target is preparing.PrepareSQLTarget.schemas:
    preparer.create_schemas(drop_if_exist=drop_if_exist)

  elif target is preparing.PrepareSQLTarget.tables:
    preparer.create_tables(drop_if_exist=drop_if_exist, relations=relations)

  elif target is preparing.PrepareSQLTarget.materialized_views:
    preparer.create_materialized_views(drop_if_exist=drop_if_exist, relations=relations)

  elif target is preparing.PrepareSQLTarget.views:
    preparer.create_views(drop_if_exist=drop_if_exist, replace_if_exist=replace_if_exist, relations=relations)

  elif target is preparing.PrepareSQLTarget.procedures:
    preparer.create_procedures(drop_if_exist=drop_if_exist, replace_if_exist=replace_if_exist, relations=relations)

  elif target is preparing.PrepareSQLTarget.functions:
    preparer.create_functions(drop_if_exist=drop_if_exist, replace_if_exist=replace_if_exist, relations=relations)

all_database_values = sql_config.keys()
all_targets = [t.value for t in preparing.PrepareSQLTarget if t]
preparer = preparing.SQLPreparer()
target_relations = {
  'settings': [],
  'schemas': [],
  'tables': [r.name for r in preparer.tables],
  'functions': [r.name for r in preparer.functions],
  'materialized_views': [r.name for r in preparer.materialized_views],
  'views': [r.name for r in preparer.views],
  'procedures': [r.name for r in preparer.procedures],
}
all_relations = [
  *target_relations['settings'],
  *target_relations['schemas'],
  *target_relations['tables'],
  *target_relations['functions'],
  *target_relations['materialized_views'],
  *target_relations['views'],
  *target_relations['procedures'],
]
del preparer

def configure_sql(config: Dict[str, any]):
  models.SQL.Layer.configure_connection(config)

def filtered_names(names: Tuple[str], filter: Tuple[str], exclude_filter: Tuple[str]) -> Tuple[str]:
  return tuple(n for n in names if (not filter or n in filter) and n not in exclude_filter)

def prepare_schema(schema_name: str, target_name: str, drop_if_exist: bool=False, replace_if_exist: bool=True, relations: List[str]=[]):
  preparer = preparing.SQLPreparer(sql_layer=models.SQL.Layer(echo=True), schema_name=schema_name)
  target = preparing.PrepareSQLTarget(target_name)
  run_sql_action(preparer=preparer, target=target, drop_if_exist=drop_if_exist, replace_if_exist=replace_if_exist, relations=relations)

def maintain_schema(schema_name: str, sql: str):
  preparer = preparing.SQLPreparer(sql_layer=models.SQL.Layer(echo=True), schema_name=schema_name)
  preparer.run_custom_sql(sql)

def load_schema(schema_name: str, unload_path: Optional[str]=None, load_path: Optional[str]=None, drop_if_exist: bool=False, relations: List[str]=[]):
  preparer = preparing.SQLPreparer(sql_layer=models.SQL.Layer(echo=True), schema_name=schema_name)
  if unload_path:
    preparer.unload_tables(path=unload_path, drop_if_exist=drop_if_exist, relations=relations)
  if load_path:
    preparer.load_tables(path=load_path, drop_if_exist=drop_if_exist, relations=relations)

def confirm(prompt: str, require_code: bool=False, bypass: bool=False):
  print(f'\nbypass: {bypass}\n')
  if bypass:
    return True

  if not require_code:
    return click.prompt(prompt, type=click.Choice(['y', 'n']), confirmation_prompt=True) == 'y'
  seed = randint(1, 9)
  confirmation = f'{seed}{(seed + randint(4, 5)) % 10}{(seed + randint(1, 9)) % 10}'
  return click.prompt(prompt, confirmation_prompt=True, type=click.Choice([confirmation, 'n'])) == confirmation

def record_best_effort_error(error: Exception, record: Dict[str, any], data_base: str, schema: str, action: str, target: Optional[str]=None):
  if isinstance(error, KeyboardInterrupt):
    raise error

  if data_base not in record:
    record[data_base] = {}
  if schema not in record[data_base]:
    record[data_base][schema] = []
  record[data_base][schema].append({
    'action': action,
    'target': target,
    'error': error,
  })

  print(Status.error.text(str(error)))
  print('Continuing with best effort after error\n')
  
def present_best_effort_errors(record: Dict[str, any]):
  error_count = 0
  df = pd.DataFrame()
  now = datetime.utcnow()
  for data_base in sorted(record.keys()):
    print(Status.warning.text(f'\n\n{data_base}: Errors occurred for {len(record[data_base].keys())} schemas ({",".join(record[data_base].keys())})'))
    for schema in sorted(record[data_base].keys()):
      error_count += len(record[data_base][schema])
      print(Status.warning.text(f'\n{data_base}.{schema}: {len(record[data_base][schema])} errors occurred for schema {schema}'))
      for item in record[data_base][schema]:
        context = [s for s in [data_base, schema, item['target']] if s is not None]
        df = df.append([{
          'date': now,
          'database': data_base,
          'schema': schema,
          'action': item['action'],
          'target': item['target'],
          'error': type(item["error"]).__name__,
          'detail': repr(item['error'])
        }])
        print(Status.warning.text(f'{".".join(context)}: {type(item["error"]).__name__}'))
        print(Status.error.text(str(item['error'])))
  if record:
    path = os.path.join('output', 'csv', f'{now.isoformat().replace(":", "_").replace(".", "_d")}_prepare_errors.csv')
    df.to_csv(path)
    print(Status.error.text(f'Best effort attempt completed with {error_count} errors.\nA record has been saved to {path}'))
    raise ValueError('Errors occurred during best effort attempt', record)

@click.command()
@click.option('-c', '--company', 'company_filter', type=click.Choice(all_config_values['companies']), multiple=True)
@click.option('-xc', '--exclude-company', 'exclude_company_filter', type=click.Choice(all_config_values['companies']), multiple=True)
@click.option('-t', '--target', 'target_filter', type=click.Choice(all_targets), multiple=True)
@click.option('-xt', '--exclude-target', 'exclude_target_filter', type=click.Choice(all_targets), multiple=True)
@click.option('-r', '--relation', 'relation_filter', type=click.Choice(all_relations), multiple=True)
@click.option('-xr', '--exclude_relation', 'exclude_relation_filter', type=click.Choice(all_relations), multiple=True)
@click.option('-db', '--database', 'database_filter', type=click.Choice(all_database_values), multiple=True)
@click.option('-xdb', '--exclude-database', 'exclude_database_filter', type=click.Choice(all_database_values), multiple=True)
@click.option('-s', '--schema', 'schema_name', type=str, default='')
@click.option('-d', '--drop', 'should_drop', is_flag=True)
@click.option('--replace/--no-replace', 'should_replace', is_flag=True, default=True)
@click.option('-f', '--sql-file', 'sql_file', type=str, default='')
@click.option('-l', '--load', 'load_path', type=str, default='')
@click.option('-u', '--unload', 'unload_path', type=str, default='')
@click.option('-be', '--best-effort', 'best_effort', is_flag=True)
@click.option('--use-the-force', 'bypass_confirmation', is_flag=True)
def run(
  company_filter: Tuple[str],
  exclude_company_filter: Tuple[str],
  target_filter: Tuple[str],
  exclude_target_filter: Tuple[str], 
  relation_filter: Tuple[str],
  exclude_relation_filter: Tuple[str],
  database_filter: Tuple[str],
  exclude_database_filter: Tuple[str],
  schema_name: str, 
  should_drop: bool,
  should_replace: bool, 
  sql_file: str,
  load_path: str,
  unload_path: str,
  best_effort: bool,
  bypass_confirmation: bool
):
  print(Status.warning.text('Warning: CTRL-C may not interrupt this script during database queryies.\nUse CTRL-Z then kill the process if necessary.\nSee http://initd.org/psycopg/docs/faq.html#best-practices and http://initd.org/psycopg/docs/advanced.html#support-for-coroutine-libraries'))
  if not database_filter and not exclude_database_filter:
    database_filter = 'default'

  if relation_filter or exclude_relation_filter:
    for target in all_targets:
      if target not in exclude_target_filter and not target_relations[target]:
        exclude_target_filter += (target,)

  filtered_targets = filtered_names(all_targets, target_filter, exclude_target_filter)
  targets_text = ',\n'.join([
    '{target}{relations}'.format(
      target=t,
      relations=' [' + ', '.join(filtered_names(names=target_relations[t], filter=relation_filter, exclude_filter=exclude_relation_filter)) + ']' if t in target_relations else ''
    )
    for t in filtered_targets
  ])

  best_effort_errors = {}
  filtered_databases = filtered_names(all_database_values, database_filter, exclude_database_filter)
  for database in filtered_databases:
    config = sql_config[database]    
    database_name = config['database']
    print(f'Connecting to database {database} ({database_name})...')
    configure_sql(config)

    if sql_file:
      if os.path.isdir(sql_file):
        file_paths = glob.glob(os.path.join(sql_file, '*.sql'))
        sql_files = {
          os.path.basename(f): Path(f).read_text()
          for f in file_paths
        }
      else:
        sql_files = {sql_file: Path(sql_file).read_text()}
      newline = '\n'
      print(f'SQL file contents:\n{(newline * 3).join(f"> {f}:{newline * 2}{sql_files[f]}" for f in sql_files)}\n')

    if schema_name:
      company_names = [schema_name]
      filtered_configs = {schema_name: CompanyConfigurations.for_schema(schema=schema_name)}
    else:
      company_names = filtered_names(all_config_values['companies'], company_filter, exclude_company_filter)
      filtered_configs = OrderedDict([(f, company_configs[f].filtered(companies_filter=company_names)) for f in sorted(company_configs.keys())])
    companies_text = ', '.join(i for c in filtered_configs.values() for i in c.company_identifiers)
    companies_or_schema = 'schema' if schema_name else 'companies'
    if sql_file:
      if confirm(prompt=f'Confirm run SQL files ({", ".join(sql_files.keys())}) for {companies_or_schema} {companies_text}', require_code=len(company_names) > 1, bypass=bypass_confirmation):
        for configs in filtered_configs.values():
          for config in configs.configs:
            for file_path in sorted(sql_files.keys()):
              print(f'Running SQL file {file_path} for {config.metadata.schema}')
              sql = sql_files[file_path]
              if best_effort:
                try:
                  maintain_schema(schema_name=config.metadata.schema, sql=sql)
                except (KeyboardInterrupt, SystemExit):
                  raise
                except Exception as e:
                  record_best_effort_error(error=e, record=best_effort_errors, data_base=database_name, schema=config.metadata.schema, action='file', target=file_path)
              else:
                maintain_schema(schema_name=config.metadata.schema, sql=sql)
      present_best_effort_errors(record=best_effort_errors)
      return
    if unload_path or load_path:
      tables = filtered_names(names=target_relations['tables'], filter=relation_filter, exclude_filter=exclude_relation_filter)
      confirmation = 'Confirm {unload}{unload_suffix}{drop}{load} for {companies_or_schema} {companies} tables [{tables}]'.format(
        unload=f'unload to {unload_path}' if unload_path else '',
        unload_suffix=' then ' if unload_path and load_path else '',
        drop='drop data then ' if should_drop and load_path else '',
        load=f'load from {load_path}' if load_path else '',
        companies_or_schema=companies_or_schema,
        companies=companies_text,
        tables=', '.join(tables)
      )
      if confirm(prompt=confirmation, require_code=len(company_names) > 1, bypass=bypass_confirmation):
        for configs in filtered_configs.values():
          for config in configs.configs:
            if best_effort:
              for table in tables:
                try:
                  load_schema(schema_name=config.metadata.schema, unload_path=unload_path, load_path=load_path, drop_if_exist=should_drop, relations=[table])
                except Exception as e:
                  record_best_effort_error(error=e, record=best_effort_errors, data_base=database_name, schema=config.metadata.schema, action='load' if load_path else 'unload', target=table)
            else:
              load_schema(schema_name=config.metadata.schema, unload_path=unload_path, load_path=load_path, drop_if_exist=should_drop, relations=list(tables))
      present_best_effort_errors(record=best_effort_errors)
      return
    if should_drop:
      should_drop = confirm(prompt=f'Confirm drop target ({targets_text}) for {companies_or_schema} {companies_text}', require_code=len(company_names) > 1, bypass=bypass_confirmation)
    for configs in filtered_configs.values():
      for config in configs.configs:
        for target in filtered_targets:
          relations = list(filtered_names(names=target_relations[target], filter=relation_filter, exclude_filter=exclude_relation_filter))
          if best_effort:
            for relation in relations:
              try:
                prepare_schema(
                  schema_name=config.metadata.schema, 
                  target_name=target, 
                  drop_if_exist=should_drop,
                  replace_if_exist=should_replace,
                  relations=[relation]
                )
              except Exception as e:
                record_best_effort_error(error=e, record=best_effort_errors, data_base=database_name, schema=config.metadata.schema, action='prepare', target=relation)
          else:
            prepare_schema(
              schema_name=config.metadata.schema, 
              target_name=target, 
              drop_if_exist=should_drop,
              replace_if_exist=should_replace,
              relations=relations
            )
  present_best_effort_errors(record=best_effort_errors)

if __name__ == "__main__":
  run()