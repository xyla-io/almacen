import control
import models
import logging
import click
import json
import os
import io
import subprocess
import pandas as pd

from math import ceil
from config import sql_config, company_configs, company_config_values, CompanyConfigurations
from typing import Optional, Dict, Tuple, Union
from datetime import datetime, timedelta
from pdb import bdb
from time import sleep
from typing import Tuple, List, Callable
from moda.style import Format
from credentials import access_credentials
from data_layer import locator_factory, ResourceLocator, Decryptor
from fabrica import run as run_fabrica
from pathlib import Path

all_database_values = sql_config.keys()

def validate_repeat_count(ctx, param, value):
  if value < 1:
    raise click.BadParameter('repeat must be a positive number')
  return value

def validate_date_string(ctx, param, value):
  if not value:
    return None
  try:
    date = datetime.strptime(value, '%Y-%m-%d')
  except ValueError:
    raise click.BadParameter('date must be have the format YYYY-MM-DD')
  return date

def validate_interval(ctx, param, value):
  if value < 1:
    raise click.BadParameter('interval must be a positive integer')
  return value

class Almacen:
  repeat_count: int
  retry_count: int
  database_name: str
  use_best_effort: bool
  use_the_force: bool
  best_effort_errors: Dict[str, any]
  access_url: Optional[str]
  access_secrets: Dict[str, str]

  def __init__(self, database_name: str, repeat_count: int, retry_count: int, use_best_effort: bool, use_the_force: bool, access_url: str, access_secrets: Dict[str, any]={}):
    self.database_name = database_name
    self.repeat_count = repeat_count
    self.retry_count = retry_count
    self.use_best_effort = use_best_effort
    self.use_the_force = use_the_force
    self.best_effort_errors = {}
    self.access_url = access_url
    self.access_secrets = {**access_secrets}
  
  def configure_access(self):
    access_credentials['credentials_url'] = self.access_url
    access_credentials['secrets'] = self.access_secrets
    for alias, url in access_credentials['aliases'].items():
      ResourceLocator.register_url(
        alias=alias,
        url=url
      )
    for name, private_key_url in access_credentials['secrets'].items():
      locator = locator_factory(url=private_key_url)
      private_key_bytes = locator.get()
      password = locator.get_locator_parameter(parameter='password')
      decryptor = Decryptor(
        private_key=private_key_bytes,
        password=password.encode(),
        name=name
      )
      Decryptor.register_decryptor(decryptor=decryptor)

  def configure_sql(self):
    config = sql_config[self.database_name]
    database_colored = Format().yellow()(str(config['database']))
    print(f'connecting to database: {database_colored}...')
    models.SQL.Layer.configure_connection(config)

  def configure_fabrica(self):
    fabrica_config_path = Path(__file__).parent / 'output' / 'temp' / 'fabrica.json'
    fabrica_config = sql_config
    with open(fabrica_config_path, 'w') as f:
      json.dump(fabrica_config, f)
    os.environ['FABRICA_SQL_CONFIG_PATH'] = str(fabrica_config_path)

  def configure_logging(self, output_filename: str):
    logging.basicConfig(
      filename=output_filename,
      level=logging.INFO,
      filemode='w',
      datefmt='%m/%d/%y %I:%M:%S %p',
      format='%(asctime)s | %(message)s'
    )

  def should_skip(self, target: str, filter: Tuple[str], exclude_filter: Tuple[str]) -> bool:
    return any([
      filter and target not in filter,
      target in exclude_filter
    ])

  def filtered_names(self, names: Tuple[str], filter: Tuple[str], exclude_filter: Tuple[str]) -> Tuple[str]:
    return tuple(n for n in names if (not filter or n in filter) and n not in exclude_filter)
  
  def filtered_actions(self, filter: Tuple[str], exclude_filter: Tuple[str], all_config_values: Dict[str, List[str]]) -> List[models.ReportAction]:
    return [
      models.ReportAction(a) for a in self.filtered_names(
        names=all_config_values['actions'], 
        filter=filter, 
        exclude_filter=exclude_filter
      )
    ]
  
  def filtered_targets(self, filter: Tuple[str], exclude_filter: Tuple[str], all_config_values: Dict[str, List[str]]) -> List[models.ReportAction]:
    return [
      models.ReportTarget(t) for t in self.filtered_names(
        names=all_config_values['targets'], 
        filter=filter, 
        exclude_filter=exclude_filter
      )
    ]
  
  def filtered_task_sets(self, filter: Tuple[str], exclude_filter: Tuple[str], all_config_values: Dict[str, List[str]]) -> List[models.ReportAction]:
    return list(self.filtered_names(
      names=all_config_values['task_sets'], 
      filter=filter, 
      exclude_filter=exclude_filter
    ))
  
  def filtered_task_types(self, filter: Tuple[str], exclude_filter: Tuple[str], all_config_values: Dict[str, List[str]]) -> List[models.ReportAction]:
    return [
      models.ReportTaskType(t) for t in self.filtered_names(
        names=all_config_values['task_types'], 
        filter=filter, 
        exclude_filter=exclude_filter
      )
    ]

  def record_best_effort_error(self, error: Exception, schema: str, action: str, target: Optional[str]=None):
    if isinstance(error, KeyboardInterrupt):
      raise error

    if self.database_name not in self.best_effort_errors:
      self.best_effort_errors[self.database_name] = {}
    if schema not in self.best_effort_errors[self.database_name]:
      self.best_effort_errors[self.database_name][schema] = []
    self.best_effort_errors[self.database_name][schema].append({
      'action': action,
      'target': target,
      'error': error,
    })

    print(Format().red()(str(error)))
    print('Continuing with best effort after error\n')
    
  def present_best_effort_errors(self):
    error_count = 0
    df = pd.DataFrame()
    now = datetime.utcnow()
    for data_base in sorted(self.best_effort_errors.keys()):
      print(Format().yellow()(f'\n\n{data_base}: Errors occurred for {len(self.best_effort_errors[data_base].keys())} schemas ({",".join(self.best_effort_errors[data_base].keys())})'))
      for schema in sorted(self.best_effort_errors[data_base].keys()):
        error_count += len(self.best_effort_errors[data_base][schema])
        print(Format().yellow()(f'\n{data_base}.{schema}: {len(self.best_effort_errors[data_base][schema])} errors occurred for schema {schema}'))
        for item in self.best_effort_errors[data_base][schema]:
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
          print(Format().yellow()(f'{".".join(context)}: {type(item["error"]).__name__}'))
          print(Format().red()(str(item['error'])))
    if self.best_effort_errors:
      path = os.path.join('output', 'csv', f'{now.isoformat().replace(":", "_").replace(".", "_d")}_run_errors.csv')
      df.to_csv(path)
      print(Format().red()(f'Best effort attempt completed with {error_count} errors.\nA record has been saved to {path}'))
      raise ValueError('Errors occurred during best effort attempt', self.best_effort_errors)

pass_almacen = click.make_pass_decorator(Almacen)

class CompanyContext:
  almacen: Almacen
  file_configs: Dict[str, CompanyConfigurations]

  def __init__(self, almacen: Almacen, file_configs: Dict[str, CompanyConfigurations]):
    self.almacen = almacen
    self.file_configs = file_configs

  def task_set_result_message(self, controller: control.CompanyController, error: Optional[Exception]=None) -> str:
    assert len(controller.configuration.task_sets) == 1
    task_set = controller.configuration.task_sets[0]
    return f'{("—" if not error else "=") * 20} Almacen Task Set {"Completion" if not error else "Error"}: {controller.configuration.metadata.schema}.{task_set.identifier}{f" ({type(error).__name__})" if error else ""}'
  
  def run_configs(self, callback: Callable[[control.CompanyController], None], echo: bool=True):
    for file, file_config in self.file_configs.items():
      for config_index, config in enumerate(file_config.configs):
        for repetition in range(0, self.almacen.repeat_count):
          repetition_text = f' (repetition {repetition + 1} of {self.almacen.repeat_count})' if self.almacen.repeat_count > 1 else ''

          if echo:
            print(f'running --> {file}:{config_index} | {config.identifier} {repetition_text}')
            print(f'running task sets --> {", ".join(config.task_set_identifiers)}')
          for task_set in config.task_sets:
            for attempt in range(0, self.almacen.retry_count + 1):
              try:
                if echo:
                  print(f'running task set --> {task_set.as_dict}')
                filtered_config = config.filtered(task_sets_filter=[task_set.identifier])
                controller = control.CompanyController(configuration=filtered_config)
                callback(controller)
                logging.info(f'\n\n{self.task_set_result_message(controller=controller)}\n\n')
                break
              except KeyboardInterrupt: raise
              except bdb.BdbQuit: raise
              except Exception as e:
                logging.error(e)
                if attempt < self.almacen.retry_count:
                  if echo:
                    print(f'retrying --> {file}:{config_index} | {task_set.identifier} {repetition_text} (attempt {attempt + 2} of {self.almacen.retry_count + 1})')
                  sleep(10)
                else:
                  logging.error(f'\n\n{self.task_set_result_message(controller=controller, error=e)}\n\n')
                  if self.almacen.use_best_effort:
                    self.almacen.record_best_effort_error(
                      error=e,
                      schema=controller.configuration.metadata.schema,
                      action=task_set.action.value,
                      target=task_set.identifier
                    )
                  else:
                    raise


@click.group()
@click.option('-db', '--database', 'database', type=click.Choice(all_database_values), default='default')
@click.option('-r', '--repeat', 'repeat_count', type=int, default=1, callback=validate_repeat_count)
@click.option('-rt', '--retry', 'retry_count', type=int, default=0)
@click.option('-be', '--best-effort', 'best_effort', is_flag=True)
@click.option('--use-the-force', 'use_the_force', is_flag=True)
@click.option('-au', '--access-url', 'access_url')
@click.option('-ap', '--access-url-prompt', 'access_url_prompt', is_flag=True)
@click.option('-sn', '--access-secret-name', 'access_secret_names', multiple=True)
@click.option('-su', '--access-secret-url', 'access_secret_urls', multiple=True)
@click.pass_context
def run(ctx: any, database: str, repeat_count: int, retry_count: int, best_effort: bool, use_the_force: bool, access_url: str, access_url_prompt: bool, access_secret_names: Tuple[str], access_secret_urls: Tuple[str]):
  assert len(access_secret_names) >= len(access_secret_urls)
  while len(access_secret_urls) < len(access_secret_names):
    access_secret_urls += (click.prompt(f"Access secret URL for name '{access_secret_names[len(access_secret_urls)]}'", hide_input=True),)
  if access_url_prompt:
    access_url = click.prompt('Access URL', hide_input=True)

  almacen = Almacen(
    database_name=database,
    repeat_count=repeat_count,
    retry_count=retry_count,
    use_best_effort=best_effort,
    use_the_force=use_the_force,
    access_url=access_url if access_url is not None else access_credentials['credentials_url'],
    access_secrets=dict(zip(access_secret_names, access_secret_urls)) if access_secret_names else access_credentials['secrets']
  )

  almacen.configure_access()
  almacen.configure_sql()
  almacen.configure_fabrica()
  almacen.configure_logging('output/log/almacen_output.log')
  ctx.obj = almacen

@run.resultcallback()
def finish_run(result: any, **kwargs):
  almacen = click.get_current_context().obj
  if almacen.use_best_effort:
    almacen.present_best_effort_errors()

@run.group()
@click.option('-f', '--file', 'file_filter', type=str, multiple=True)
@click.option('-xf', '--exclude-file', 'exclude_file_filter', type=str, multiple=True)
@click.option('-n', '--name', 'company_filter', type=str, multiple=True)
@click.option('-xn', '--exclude-name', 'exclude_company_filter', type=str, multiple=True)
@click.option('-a', '--action', 'action_filter', type=str, multiple=True)
@click.option('-xa', '--exclude-action', 'exclude_action_filter', type=str, multiple=True)
@click.option('-t', '--target', 'target_filter', type=str, multiple=True)
@click.option('-xt', '--exclude-target', 'exclude_target_filter', type=str, multiple=True)
@click.option('-s', '--taskset', 'task_set_filter', type=str, multiple=True)
@click.option('-xs', '--exclude-taskset', 'exclude_task_set_filter', type=str, multiple=True)
@click.option('-j', '--job', 'task_type_filter', type=str, multiple=True)
@click.option('-xj', '--exclude-job', 'exclude_task_type_filter', type=str, multiple=True)
@click.option('-c', '--config', 'configs', type=click.File(), multiple=True)
@click.option('-o', '--config-out', 'config_output_path', type=str)
@click.pass_obj
@click.pass_context
def company(
  ctx: any,
  almacen: Almacen,
  file_filter: Tuple[str],
  exclude_file_filter: Tuple[str],
  company_filter: Tuple[str],
  exclude_company_filter: Tuple[str],
  action_filter: Tuple[str],
  exclude_action_filter: Tuple[str],
  target_filter: Tuple[str],
  exclude_target_filter: Tuple[str],
  task_set_filter: Tuple[str],
  exclude_task_set_filter: Tuple[str],
  task_type_filter: Tuple[str],
  exclude_task_type_filter: Tuple[str],
  configs: List[io.TextIOWrapper],
  config_output_path: str
):
  # TODO:
  # add support for a '-u/--url' option that allows you to pass in url (which can be local) for company configurations
  # this will likely replace the '-c/--config' flag

  all_configs = {f.name: CompanyConfigurations.from_json(f.read()) for f in configs} if configs else company_configs
  all_config_values = company_config_values(all_configs)
  filtered_configs = {
    file: all_configs[file].filtered(
      companies_filter=almacen.filtered_names(all_configs[file].company_identifiers, company_filter, exclude_company_filter),
      actions_filter=almacen.filtered_actions(action_filter, exclude_action_filter, all_config_values),
      targets_filter=almacen.filtered_targets(target_filter, exclude_target_filter, all_config_values),
      task_sets_filter=almacen.filtered_task_sets(task_set_filter, exclude_task_set_filter, all_config_values),
      task_types_filter=almacen.filtered_task_types(task_type_filter, exclude_task_type_filter, all_config_values)
    )
    for file in sorted(all_configs.keys()) if not almacen.should_skip(file, file_filter, exclude_file_filter)
  }
  if config_output_path:
    with open(config_output_path, 'w') as f:
      json.dump({p: c.as_dict for p, c in filtered_configs.items()}, f, sort_keys=True, indent=2)
  ctx.obj = CompanyContext(almacen=almacen, file_configs=filtered_configs)

@company.command()
@click.option('-d', '--date', 'date', type=str, default='', callback=validate_date_string)
@click.option('--start-date', 'start_date', type=str, default='', callback=validate_date_string)
@click.option('--end-date', 'end_date', type=str, default='', callback=validate_date_string)
@click.pass_obj
@pass_almacen
def fill(almacen: Almacen, company_context: CompanyContext, date: Optional[datetime], start_date: Optional[datetime], end_date: Optional[datetime]):
  if date is not None:
    models.TimeModel.utc_now = date
  if start_date is not None:
    models.TimeModel.start_date = start_date
  if end_date is not None:
    models.TimeModel.end_date = end_date
  
  company_context.run_configs(lambda controller: controller.run_tasks())

@company.command()
@click.option('-d', '--date', 'date', type=str, callback=validate_date_string)
@click.option('--start-date', 'start_date', type=str, default='', callback=validate_date_string)
@click.option('--end-date', 'end_date', type=str, default='', callback=validate_date_string)
@click.pass_obj
@pass_almacen
def backfill(almacen: Almacen, company_context: CompanyContext, date: Optional[datetime], start_date: Optional[datetime], end_date: Optional[datetime]):
  models.TimeModel.backfill_target_date = date
  if start_date is not None:
    models.TimeModel.start_date = start_date
  if end_date is not None:
    models.TimeModel.end_date = end_date

  company_context.run_configs(lambda controller: controller.run_tasks())

@company.command()
@click.option('--start-date', 'start_date', type=str, callback=validate_date_string, prompt=True)
@click.option('--end-date', 'end_date', type=str, callback=validate_date_string, prompt=True)
@click.option('--interval', 'interval', type=int, callback=validate_interval, default=30)
@click.pass_obj
@pass_almacen
def fillrange(almacen: Almacen, company_context: CompanyContext, start_date: datetime, end_date: datetime, interval: int):
  start = start_date
  end = end_date
  repetitions = ceil((end_date - start_date).days / interval)
  if repetitions < 1:
    print('No range to fill.')
    return

  count = 0
  while True:
    count += 1
    if (end - start).days >= interval:
      end = start + timedelta(days=interval - 1)
    print(Format().yellow()(f'Filling interval {count} of {repetitions} ({start.strftime("%Y-%m-%d")} through {end.strftime("%Y-%m-%d")}).'))
    models.TimeModel.start_date = start
    models.TimeModel.end_date = end
    company_context.run_configs(lambda controller: controller.run_tasks())
    if end == end_date:
      break
    start = end + timedelta(days=1)
    end = end_date

@company.command()
@click.option('--start-date', 'start_date', type=str, callback=validate_date_string, prompt=True)
@click.option('--end-date', 'end_date', type=str, callback=validate_date_string, prompt=True)
@click.pass_obj
@pass_almacen
@click.pass_context
def materialize_cube(ctx: any, almacen: Almacen, company_context: CompanyContext, start_date: datetime, end_date: datetime):
  if end_date < start_date:
    return
  configs = company_context.file_configs
  company_context.file_configs = {
    file: configs[file].filtered(
      targets_filter=[
        models.ReportTarget.performance_cube_unfiltered
      ]
    )
    for file in sorted(configs.keys())
  }
  print(Format().yellow()('Rematerializing the unfiltered performance cube'))
  ctx.invoke(
    fillrange,
    start_date=start_date,
    end_date=end_date,
    interval=48
  )
  company_context.file_configs = {
    file: configs[file].filtered(
      targets_filter=[
        models.ReportTarget.performance_cube_filtered
      ],
      task_types_filter=[
        models.ReportTaskType.mutate_filter_performance_cube_channels,
        models.ReportTaskType.mutate_filter_performance_cube_mmps,
        models.ReportTaskType.mutate_filter_performance_cube_organic,
      ]
    )
    for file in sorted(configs.keys())
  }
  print(Format().yellow()('Rematerializing the filtered performance cube'))
  ctx.invoke(
    fillrange,
    start_date=start_date,
    end_date=end_date - timedelta(days=48),
    interval=48
  )
  company_context.file_configs = {
    file: configs[file].filtered(
      targets_filter=[
        models.ReportTarget.performance_cube_filtered
      ],
    )
    for file in sorted(configs.keys())
  }
  print(Format().yellow()('Rematerializing the unfiltered performance cube and updating globals'))
  ctx.invoke(
    fill,
    date=None,
    start_date=max(start_date, end_date - timedelta(days=47)),
    end_date=end_date
  )

@company.command()
@click.pass_obj
@pass_almacen
def reset(almacen: Almacen, company_context: CompanyContext):
  company_context.run_configs(lambda controller: controller.reset_tasks())

@company.command()
@click.pass_obj
@pass_almacen
def show(almacen: Almacen, company_context: CompanyContext):
  company_task_set_map = {}

  def gather_run_info(controller: control.CompanyController):
    company_name = controller.configuration.metadata.display_name
    schema_name = controller.configuration.metadata.schema
    if company_name.replace(' ', '').lower() != schema_name:
      company_name = f'{company_name} ({schema_name})'

    task_sets = controller.configuration.task_sets
    if company_name in company_task_set_map:
      company_task_set_map[company_name].extend(task_sets)
    else:
      company_task_set_map[company_name] = task_sets

  print()
  company_context.run_configs(callback=gather_run_info, echo=False)
  for company, task_sets in company_task_set_map.items():
    company_colored = Format().yellow()(str(company))
    task_set_identifiers = ', '.join([s.identifier for s in task_sets])
    print(f'-> {company_colored}: {task_set_identifiers}')

@company.command()
@click.argument('script')
@click.argument('script_args', nargs=-1)
@click.pass_obj
@pass_almacen
def script(almacen: Almacen, company_context: CompanyContext, script: str, script_args: Tuple[str]):
  from moda.user import UserInteractor
  file_keys = sorted(company_context.file_configs.keys())
  config_names = []
  for key in file_keys:
    config_names.extend(f'{key} > {c.identifier}' for c in company_context.file_configs[key].configs)
  user = UserInteractor(
    interactive=not almacen.use_the_force,
    timeout=None,
    script_directory_components=['local_aux']
  )
  if not user.present_confirmation(prompt=f'Run script for {len(config_names)} configurations: {", ".join(config_names)}', default_response=almacen.use_the_force):
    return
  user.locals = {
    **user.python_locals,
    'user': user,
    'almacen': almacen,
    'company_context': company_context,
    'SQL': models.SQL,
    'script_args': script_args,
  }
  user.run_script(script_name=script)

@run.group()
@click.pass_obj
def credentials(almacen: Almacen):
  pass

@credentials.command()
@click.option('-t', '--template-files', 'template_files', is_flag=True)
@click.argument('name')
@click.pass_obj
def init(almacen: Almacen, name: str, template_files: bool):
  init_name_url = ResourceLocator.join_path(url=almacen.access_url, path=f'{name}/')
  init_locator = locator_factory(url=init_name_url)
  init_locator.put(resource=None)
  if template_files:
    template_locator = locator_factory(url=f'{Path(__file__).parent / "credentials" / "company" / "template"}/')
    for path in template_locator.list():
      resource = locator_factory(url=ResourceLocator.join_path(url=template_locator.url, path=path)).get()
      locator_factory(url=ResourceLocator.join_path(url=init_name_url, path=path.replace('template', name))).put(resource=resource)

@credentials.command()
@click.argument('name')
@click.option('-p', '--password', 'password')
@click.option('--no-password', 'no_password', is_flag=True)
def generate_private_key(name: str, password: str, no_password: bool):
  assert not (no_password and password)
  if not password and not no_password:
    password = click.prompt('password', hide_input=True, confirmation_prompt=True)

  private_key_path = Path(__file__).parent / 'credentials' / 'rsa' / f'{name}.key'
  if private_key_path.exists():
    if private_key_path.is_dir():
      click.echo(f'A directory exists at {private_key_path}')
      raise click.Abort()
    click.confirm(f'A file already exists at {private_key_path}\nReplace it with a newly generated key?', abort=True)

  private_key = Decryptor.generate_private_key(password=password.encode())
  private_key_url = f'{private_key_path}?locator=1&filemode=600'

  locator = locator_factory(url=private_key_url)
  locator.put(resource=private_key)
  print(f'New private key written to {private_key_path}')

@credentials.command()
@click.argument('name')
@click.argument('urls', nargs=-1)
@click.pass_obj
def copy(almacen: Almacen, name: str, urls: List[str]):
  assert len(urls) <= 2
  if not urls:
    urls.append(click.prompt('from URL', hide_input=True))
  if len(urls) < 2:
    urls.append(click.prompt('to URL', hide_input=True))
  list_name_url = ResourceLocator.join_path(url=urls[0], path=f'{name}/')
  list_locator = locator_factory(url=list_name_url)
  paths = list_locator.list()
  assert paths is not None
  paths = [p for p in paths if not p.endswith('/')]
  existing_list_name_url = ResourceLocator.join_path(url=urls[1], path=f'{name}/')
  existing_list_locator = locator_factory(url=existing_list_name_url)
  existing_paths = existing_list_locator.list()
  if existing_paths is None:
    existing_list_locator.put(resource=None)
  for path in paths:
    copy_path = f'{name}/{path}'
    get_name_url = ResourceLocator.join_path(url=urls[0], path=copy_path)
    get_locator = locator_factory(url=get_name_url)
    resource = get_locator.get()
    put_name_url = ResourceLocator.join_path(url=urls[1], path=copy_path)
    put_locator = locator_factory(url=put_name_url)
    put_locator.put(resource=resource)
  if existing_paths is not None:
    for existing_path in existing_paths:
      if existing_path in paths:
        continue
      delete_path = f'{name}/{existing_path}'
      delete_name_url = ResourceLocator.join_path(url=urls[1], path=delete_path)
      delete_locator = locator_factory(url=delete_name_url)
      delete_locator.delete()

@credentials.command()
@click.argument('name')
@click.argument('url')
@click.pass_obj
@click.pass_context
def get(ctx: any, almacen: Almacen, name: str, url: str):
  if not url:
    url = click.prompt('URL', hide_input=True)
  ctx.invoke(
    copy,
    name=name,
    urls=[url, almacen.access_url]
  )

@credentials.command()
@click.argument('name')
@click.argument('url')
@click.pass_obj
@click.pass_context
def put(ctx: any, almacen: Almacen, name: str, url: str):
  if not url:
    url = click.prompt('URL', hide_input=True)
  ctx.invoke(
    copy,
    name=name,
    urls=[almacen.access_url, url]
  )

@credentials.command(name='list')
@click.argument('url')
@click.pass_obj
def list_items(almacen: Almacen, url: str):
  if not url:
    url = click.prompt('URL', hide_input=True)
  list_locator = locator_factory(url=url)
  paths = list_locator.list()
  click.echo(f'{len(paths)} items:')
  click.echo('\n'.join(paths))

@credentials.command()
@click.argument('name')
@click.argument('url')
@click.pass_obj
def delete(almacen: Almacen, name: str, url: str):
  if not url:
    url = click.prompt('URL', hide_input=True)
  delete_name_url = ResourceLocator.join_path(url=url, path=f'{name}/')
  delete_locator = locator_factory(url=delete_name_url)
  delete_locator.delete()

@run.command()
@click.option('-p/-P', '--publish/--no-publish', 'publish', is_flag=True)
@click.option('-c/-C', '--clean/--no-clean', 'clean', is_flag=True, default=True)
@click.option('-o/-O', '--open/--no-open', 'should_open', is_flag=True)
def document(publish: bool, clean: bool, should_open: bool):
  working_directory = os.path.dirname(os.path.abspath(__file__))
  if clean:
    subprocess.call(
      args=[
        'rm',
        '-rf',
        'output/documentation/build'
      ],
      cwd=working_directory
    )
  subprocess.call(
    args=[
    'sphinx-build',
    '-va',
    '-c', 'documentation',
    '.',
    'output/documentation/build/sphinx',
    ],
    cwd=working_directory
  )
  if publish:
    subprocess.call(
      args=f'''#!/bin/bash
set -e
set -x
cd output/documentation/build
cp ../../../documentation/netlify.toml ./
git init
git remote add netlify NETLIFYREPOURL
git remote update
git reset netlify/almacen
git add .
git commit -m 'Updated documentation.'
git push netlify HEAD:almacen''',
      shell=True,
      executable='/bin/bash',
      cwd=working_directory
    )
  if should_open:
    subprocess.call(
      args=[
        'open',
        'output/documentation/build/sphinx/documentation/index.html',
      ],
      cwd=working_directory
    )

run.add_command(run_fabrica, name='fabrica')

if __name__ == '__main__':
  run()