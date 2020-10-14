import tasks

from data_layer import SQLQuery
from typing import TypeVar, Generic, List, Dict, Optional
from abc import abstractmethod
from datetime import timedelta

T = TypeVar(tasks.ReportTask)
class ReportMutator(Generic[T]):
  task: T

  def __init__(self, task: T):
    self.task = task

  @abstractmethod
  def mutate(self):
    pass

class VoidReportMutator(ReportMutator[tasks.ReportTask]):
  def mutate(self):
    pass

class QueryReportMutator(Generic[T], ReportMutator[T]):
  @property 
  def query(self) -> Optional[SQLQuery]:
    raise NotImplementedError()

  @property
  def is_row_count_query(self) -> bool:
    return False

  def mutate(self):
    query = self.query

    if query is None:
      return

    self.task.sql_layer.connect()
    cursor = query.run(sql_layer=self.task.sql_layer)
    if self.is_row_count_query:
      affected_rows = cursor.fetchone()[0]
    else:
      affected_rows = cursor.rowcount

    self.task.sql_layer.commit()
    self.task.sql_layer.disconnect()

    return '{} rows affected from mutation.'.format(affected_rows)

class MaterializeMutator(Generic[T], QueryReportMutator[T]):
  @property
  def is_row_count_query(self) -> bool:
    return True

  @property
  def prepare_query(self) -> SQLQuery:
    return SQLQuery(query='')

  @property
  def insert_query(self) -> SQLQuery:
    columns = '(' + ', '.join([f'"{c}"' for c in self.source_columns]) + ') ' if self.source_columns else ''
    return SQLQuery(f'INSERT INTO {self.stage_table_name} {columns} ({self.source_query.query});', substitution_parameters=self.source_query.substitution_parameters)

  @property
  def source_columns(self) -> List[str]:
    return []

  @property
  def source_query(self) -> SQLQuery:
    raise NotImplementedError()

  @property
  def delete_query(self) -> SQLQuery:
    return SQLQuery(f'TRUNCATE TABLE {self.task.report_table_model.full_table_name};')

  @property
  def cleanup_query(self) -> SQLQuery:
    return SQLQuery(query='')

  @property
  def stage_table_name(self) -> str:
    return f'stage_{self.task.report_table_model.table_name}'

  @property
  def query(self) -> Optional[SQLQuery]:
    query = f'''
CREATE TEMPORARY TABLE {self.stage_table_name} (LIKE {self.task.report_table_model.full_table_name} INCLUDING DEFAULTS);
BEGIN TRANSACTION;
{self.prepare_query.query}
{self.insert_query.query}
{self.delete_query.query}
INSERT INTO {self.task.report_table_model.full_table_name} (SELECT * FROM {self.stage_table_name});
{self.cleanup_query.query}
END TRANSACTION;
SELECT COUNT(*) FROM {self.stage_table_name};
    '''
    return SQLQuery(
      query=query,
      substitution_parameters=(
        *self.prepare_query.substitution_parameters,
        *self.insert_query.substitution_parameters, 
        *self.delete_query.substitution_parameters,
        *self.cleanup_query.substitution_parameters
      )
    )

class AlchemyReportMutator(Generic[T], ReportMutator[T]):
  def mutate(self):
    session = self.task.sql_layer.alchemy_session()
    if self.mutate_session(session=session):
      session.commit()

  @abstractmethod
  def mutate_session(self, session: any) -> bool:
    pass

class ReplaceReportMutator(AlchemyReportMutator[T]):
  def mutate_session(self, session: any) -> bool:
    table = self.task.report_table_model.table
    query = self.task.filtered_alchemy_query_by_identifier_columns(session.query(table)) \
      .filter(table.columns[self.task.report_table_model.date_column_name] >= self.task.report_start_date) \
      .filter(table.columns[self.task.report_table_model.date_column_name] < self.task.report_end_date + timedelta(days=1))
    query.delete(synchronize_session=False)
    return True

class LatestColumnValueMutator(QueryReportMutator[tasks.FetchCampaignReportTask]):
  specifier: tasks.LatestValueMutationSpecifier

  def __init__(self, task: tasks.FetchCampaignReportTask, specifier: tasks.LatestValueMutationSpecifier):
    super().__init__(task=task)
    self.specifier = specifier

  @property
  def query(self) -> Optional[SQLQuery]:
    if self.specifier.only_missing_values:
      missing_source_condition = SQLQuery.array_condition_query(
        expression=f'"{self.specifier.source_value_column}"', 
        values=self.specifier.only_missing_values, 
        negate=True
      )
      missing_source_condition.query = f'\nAND {missing_source_condition.query}'
      missing_target_condition = SQLQuery.array_condition_query(
        expression=f'{self.task.report_table_model.full_table_name}."{self.specifier.target_value_column}"', 
        values=self.specifier.only_missing_values
      )
      missing_target_condition.query = f'\nAND {missing_target_condition.query}'
    else:
      missing_source_condition = SQLQuery('')      
      missing_target_condition = SQLQuery('')

    task_identifier_conditions = SQLQuery('')
    self.task.append_identifier_column_conditions_to_query(task_identifier_conditions)
    query = """
update {table_name}
set "{target_column}" = latest."{target_column}"
from
(
  select "{identifier_column}", max("{source_column}") as "{target_column}"
	from
    (
      (
        select "{identifier_column}", max("{date_column}") as "{date_column}"
        from {table_name}
        where {task_identifier_conditions}{missing_source_condition}
        group by "{identifier_column}"
      ) as last_date
      left join
      {table_name}
      using ("{identifier_column}", "{date_column}")
    )
    group by "{identifier_column}"
) as latest
where {table_name}."{identifier_column}" = latest."{identifier_column}"
and {task_identifier_conditions}{missing_target_condition}
    """.format(
      table_name=self.task.report_table_model.full_table_name,
      identifier_column=self.specifier.identifier_column,
      source_column=self.specifier.source_value_column,
      target_column=self.specifier.target_value_column,
      date_column=self.task.report_table_model.date_column_name,
      task_identifier_conditions=task_identifier_conditions.query,
      missing_source_condition=missing_source_condition.query,
      missing_target_condition=missing_target_condition.query
    )

    return SQLQuery(
      query=query, 
      substitution_parameters=task_identifier_conditions.substitution_parameters + missing_source_condition.substitution_parameters + task_identifier_conditions.substitution_parameters + missing_target_condition.substitution_parameters
    )

class CurrencyConversionMutator(QueryReportMutator[tasks.FetchCampaignReportTask]):
  @property
  def query(self) -> Optional[SQLQuery]:
    task_identifier_conditions = SQLQuery('')
    self.task.append_identifier_column_conditions_to_query(task_identifier_conditions)
    query = """
update {table_name}
set currency_conversion_rate = conversion.rate,
  {money_column_updates}
from 
(
  select conversion_dates.original_currency, conversion_dates.converted_currency, conversion_dates.date, e.rate, e.crystallized
	from
    (
      select distinct "{fetched_currency_column}" as original_currency, converted_currency, "{date_column}" as date
      from {table_name}
      where currency_conversion_rate is null
      and {task_identifier_conditions}
    ) as conversion_dates
    left join {table_schema}fetch_currency_exchange_rates as e
    on conversion_dates.original_currency = e.base and conversion_dates.converted_currency = e.target and conversion_dates."date" = e."date"    
) as conversion
where {table_name}."{fetched_currency_column}" = conversion.original_currency
  and {table_name}.converted_currency = conversion.converted_currency
  and {table_name}."{date_column}" = conversion.date
  and conversion.rate is not null
  and conversion.rate != 0
  and (conversion.crystallized or not {table_name}."{crystallized_column}")
  and {task_identifier_conditions}
    """.format(
      table_name=self.task.report_table_model.full_table_name,
      table_schema=f'{self.task.report_table_schema}.' if self.task.report_table_schema is not None else '',
      fetched_currency_column=self.task.fetched_currency_column,
      date_column=self.task.report_table_model.date_column_name,
      money_column_updates=',\n'.join([
        '"{money_column}" = "{money_column}" * conversion.rate'.format(
          money_column=m
        )
        for m in self.task.money_columns
      ]),
      crystallized_column=self.task.report_table_model.crystallized_column_name,
      task_identifier_conditions=task_identifier_conditions.query
    )
    return SQLQuery(
      query=query,
      substitution_parameters=task_identifier_conditions.substitution_parameters * 2
      )
    
  def mutate(self):
    if not self.task.money_columns:
      return 'No money columns to convert.'
    
    return super().mutate()