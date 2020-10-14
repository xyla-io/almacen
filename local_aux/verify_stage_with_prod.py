import subprocess

from enum import Enum
from typing import List
from datetime import datetime, timedelta

date_format = '%Y-%m-%d'

def unload(schema: str, relations: List[str]):
  relation_options = [
    o
    for r in relations
    for o in ['-r', r]
  ]
  subprocess.call(
    args=[
      'python',
      'prepare.py',
      '-be',
      '-db', 'prod_01',
      '-c', schema,
      '-u', 'unload/prod_01',
      *relation_options,
      '--use-the-force',
    ]
  )

def drop(schema: str, relations: List[str]):
  relation_options = [
    o
    for r in relations
    for o in ['-r', r]
  ]
  subprocess.call(
    args=[
      'python',
      'prepare.py',
      '-db', 'stage_01',
      '-c', schema,
      '-d',
      *relation_options,
      '--use-the-force',
    ]
  )
  subprocess.call(
    args=[
      'python',
      'prepare.py',
      '-db', 'stage_01',
      '-c', schema,
      '--use-the-force',
    ]
  )

def load(schema: str, relations: List[str]):
  relation_options = [
    o
    for r in relations
    for o in ['-r', r]
  ]
  subprocess.call(
    args=[
      'python',
      'prepare.py',
      '-be',
      '-db', 'stage_01',
      '-c', schema,
      '-l', 'unload/prod_01',
      '-xr', 'view_windows',
      *relation_options,
      '--use-the-force',
    ]
  )

def fill(config: any, start_date: datetime, end_date: datetime, interval: int):
  task_set_options = [
    o
    for s in config.task_set_identifiers
    for o in ['-s', s]
  ]
  task_type_options = [
    o
    for j in config.task_type_identifiers
    for o in ['-j', j]
  ]
  if not task_type_options:
    return

  subprocess.call(
    args=[
      'python',
      'almacen.py',
      '-db', 'stage_01',
      '-rt', '1',
      '-be',
      'company',
      '-n', config.metadata.schema,
      *task_set_options,
      *task_type_options,
      'fillrange',
      '--start-date', start_date.strftime(date_format),
      '--end-date', end_date.strftime(date_format),
      '--interval', str(interval),
    ]
  )

def verify(schema: str, targets: List[str], verify_options: List[str]):
  return_code = subprocess.call(
    args=[
      'python',
      'almacen.py',
      'fabrica',
      '-s', schema,
      'verify',
      *(['-I'] if almacen.use_the_force else []),
      '-df', 'opendiff',
      '-db1', 'stage_01',
      '-db2', 'prod_01',
      '-E1',
      '-E2',
      *verify_options,
      *[f'local_aux/sql/verify/verify_{t}.sql' for t in targets],
    ]
  )
  if return_code:
    raise ValueError('Verification return code is not zero', return_code)

class Stage(Enum):
  unload = 'unload'
  drop = 'drop'
  load = 'load'
  fill = 'fill'
  verify = 'verify'
  verify_aggregate = 'verify-aggregate'
  verify_detail = 'verify-detail'

  def perform(self, config: any, targets: List[str], now: datetime, options: List[str]):
    if self is Stage.unload:
      unload(
        schema=config.metadata.schema,
        relations=targets
      )
    elif self is Stage.drop:
      drop(
        schema=config.metadata.schema,
        relations=targets
      )
    elif self is Stage.load:
      load(
        schema=config.metadata.schema,
        relations=targets
      )
    elif self is Stage.fill:
      start_date = datetime.strptime(options[0], date_format) if options else now - timedelta(days=4)
      end_date = datetime.strptime(options[1], date_format) if len(options) > 1 else now
      interval = int(options[2]) if len(options) > 2 else 30
      fill(
        config=config,
        start_date=start_date,
        end_date=end_date,
        interval=interval
      )
    elif self is Stage.verify:
      if not options:
        targets = ['custom'] * 2
      elif len(options) == 1:
        targets = options[0:1] * 2
      else:
        targets = options[0:2]
      verify_options = options[2:]
      verify(
        schema=config.metadata.schema,
        targets=targets,
        verify_options=verify_options
      )
    elif self is Stage.verify_aggregate:
      verify(
        schema=config.metadata.schema,
        targets=['performance_cube_filtered_aggregate'] * 2,
        verify_options=options
      )
    elif self is Stage.verify_detail:
      verify(
        schema=config.metadata.schema,
        targets=['performance_cube_filtered'] * 2,
        verify_options=options
      )

def extract_configuration_relations(config: any, options: List[str]) -> List[str]:
  relations = []
  relations.extend(j for j in config.task_type_identifiers if j.startswith('fetch_'))
  relations.extend(t for t in config.target_identifiers if t.startswith('performance_cube'))
  return relations

if script_args:
  stages = []
  for script_arg in script_args:
    parts = script_arg.split(':')
    stages.append((Stage(parts[0]), parts[1:]))
else:
  stages = [(s, []) for s in Stage]
now = datetime.utcnow()

for key in sorted(company_context.file_configs.keys()):
  for config in company_context.file_configs[key].configs:
    for stage, options in stages:
      targets = extract_configuration_relations(
        config=config,
        options=options
      )
      stage.perform(
        config=config,
        targets=targets,
        now=now,
        options=options
      )    
      