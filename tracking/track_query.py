from data_layer import Redshift as SQL

class ResetTaskHistoryQuery(SQL.GeneratedQuery):
  schema_prefix: str
  task_identifier: str
  host: str

  def __init__(self, schema_prefix: str, task_identifier: str, host: str):
    self.schema_prefix = schema_prefix
    self.task_identifier = task_identifier
    self.host = host
    super().__init__()

  def generate_query(self):
    self.query = f'''
insert into {self.schema_prefix}track_task_history
(id, creation_time, modification_time, status, task_identifier, task_name, host)
select {self.schema_prefix}uuid() as id,
  common_time as creation_time, common_time as modification_time,
  'reset' as status, task_identifier, task_name, %s as host
from (
  select task_identifier, max(modification_time) as modification_time
  from {self.schema_prefix}track_task_history
  where task_identifier = %s
  or task_identifier like %s
  group by task_identifier
)
left join (
  select id, task_identifier, modification_time, task_name
  from {self.schema_prefix}track_task_history
  where status = 'completed'
)
using (task_identifier, modification_time)
  cross join (
  select current_timestamp as common_time
)
where id is not null;
    '''
    self.substitution_parameters = (self.host, self.task_identifier, f'{self.task_identifier}.%')