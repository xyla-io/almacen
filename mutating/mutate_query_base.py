from data_layer import SQLQuery, GeneratedQuery, LiteralQuery
from typing import Optional, List, Dict, OrderedDict as OrderedDictType
from collections import OrderedDict
  
class ConditionalQuery(GeneratedQuery):
  @property
  def condition_queries(self) -> List[SQLQuery]:
    return []

  @property
  def all_conditions_query(self) -> SQLQuery:
    if not self.condition_queries:
      return SQLQuery('')
    return SQLQuery(
      query='where ' + '\nand '.join([c.query for c in self.condition_queries]),
      substitution_parameters=tuple(p for c in self.condition_queries for p in c.substitution_parameters)
    )

class SelectQuery(ConditionalQuery):
  @property
  def with_queries(self) -> OrderedDictType[str, SQLQuery]:
    return OrderedDict()

  @property
  def full_source_name(self) -> str:
    return ''

  @property
  def source_query(self) -> Optional[SQLQuery]:
    return None

  @property
  def from_query(self) -> SQLQuery:
    if self.source_query is not None:
      return SQLQuery(
        query=f'from ({self.source_query.query})',
        substitution_parameters=self.source_query.substitution_parameters
      )
    elif self.full_source_name:
      return SQLQuery(f'from {self.full_source_name}')
    else:
      return SQLQuery('')

  @property
  def source_columns(self) -> List[str]:
    return []

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return {}

  @property
  def group_by_queries(self) -> List[SQLQuery]:
    return []

  @property
  def quote_columns(self) -> bool:
    return True

  @property
  def all_withs_query(self) -> SQLQuery:
    with_queries = self.with_queries
    if not with_queries:
      return SQLQuery('')
    newline = '\n'
    return SQLQuery(
      query=f'''
with {f",{newline}".join(f"{a} as ({newline}{q.query}{newline})" for a, q in with_queries.items())}
      ''',
      substitution_parameters=tuple(p for q in with_queries.values() for p in q.substitution_parameters)
    )

  @property
  def all_columns_query(self) -> SQLQuery:
    if not self.source_columns:
      return SQLQuery('*')
    column_expressions = self.source_column_expressions
    quote = '"' if self.quote_columns else ''
    return SQLQuery(
      query=', '.join([
        f'{column_expressions[c].query} as {quote}{c}{quote}' if c in column_expressions else f'{quote}{c}{quote}'
        for c in self.source_columns
      ]),
      substitution_parameters= tuple(
        p
        for c in self.source_columns
        if c in column_expressions
        for p in column_expressions[c].substitution_parameters
      )
    )

  @property
  def all_group_by_query(self) -> SQLQuery:
    group_by_queries = self.group_by_queries
    if not group_by_queries:
      return SQLQuery('')
    else:
      return SQLQuery(
        query=f'group by {", ".join(q.query for q in group_by_queries)}',
        substitution_parameters=(p for q in group_by_queries for p in q.substitution_parameters)
      )

  def generate_query(self):
    self.query = f'''
{self.all_withs_query.query}
select {self.all_columns_query.query} 
{self.from_query.query}
{self.all_conditions_query.query}
{self.all_group_by_query.query}
    '''
    self.substitution_parameters = (
      *self.all_withs_query.substitution_parameters,
      *self.all_columns_query.substitution_parameters,
      *self.from_query.substitution_parameters,
      *self.all_conditions_query.substitution_parameters,
      *self.all_group_by_query.substitution_parameters,
    )

class CustomSelectQuery(SelectQuery):
  _with_queries: OrderedDictType[str, SQLQuery]
  _source_columns: List[str]
  _source_column_expressions: Dict[str, SQLQuery]
  _source_query: Optional[SQLQuery]
  _full_source_name: str
  _condition_queries: List[SQLQuery]
  _group_by_queries: List[SQLQuery]
  _quote_columns: bool

  def __init__(self, with_queries: OrderedDictType[str, SQLQuery]=OrderedDict(), source_columns: List[str]=[], source_column_expressions: Dict[str, SQLQuery]={}, source_query: Optional[SQLQuery]=None, full_source_name: str='', condition_queries: List[SQLQuery]=[], group_by_queries: List[SQLQuery]=[], quote_columns: bool=True):
    assert full_source_name or source_query is not None
    self._with_queries = OrderedDict(with_queries.items())
    self._source_columns = [*source_columns]
    self._source_column_expressions = {**source_column_expressions}
    self._source_query = source_query
    self._full_source_name = full_source_name
    self._condition_queries = [*condition_queries]
    self._group_by_queries = [*group_by_queries]
    self._quote_columns = quote_columns
    super().__init__()

  @property
  def with_queries(self) -> OrderedDictType[str, SQLQuery]:
    return self._with_queries

  @property
  def source_columns(self) -> List[str]:
    return self._source_columns

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    return self._source_column_expressions

  @property
  def source_query(self) -> Optional[SQLQuery]:
    return self._source_query

  @property
  def full_source_name(self) -> str:
    return self._full_source_name

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return self._condition_queries

  @property
  def group_by_queries(self) -> List[SQLQuery]:
    return self._group_by_queries

  @property
  def quote_columns(self) -> bool:
    return self._quote_columns

class UnionQuery(GeneratedQuery):
  @property
  def select_queries(self) -> List[SQLQuery]:
    return []

  def generate_query(self):
    select_queries = self.select_queries
    if not select_queries:
      return
    self.query = '\nunion all\n'.join(q.query for q in select_queries)
    self.substitution_parameters = (p for q in select_queries for p in q.substitution_parameters)

class CustomUnionQuery(UnionQuery):
  _select_queries: List[SQLQuery]

  def __init__(self, select_queries: List[SQLQuery]):
    self._select_queries = [*select_queries]
    super().__init__()

  @property
  def select_queries(self) -> List[SQLQuery]:
    return self._select_queries

class InsertQuery(GeneratedQuery):
  full_target_name: str
  select_query: SelectQuery

  def __init__(self, full_target_name: str, select_query: SelectQuery):
    self.full_target_name = full_target_name
    self.select_query = select_query
    super().__init__()

  def generate_query(self) -> SQLQuery:
    columns = '(' + ', '.join([f'"{c}"' for c in self.select_query.source_columns]) + ') ' if self.select_query.source_columns else ''
    self.query = f'''
insert into {self.full_target_name} {columns}{self.select_query.query}
    '''
    self.substitution_parameters = self.select_query.substitution_parameters

class DeleteQuery(ConditionalQuery):
  full_target_name: str

  def __init__(self, full_target_name: str):
    self.full_target_name = full_target_name
    super().__init__()

  @property
  def using_queries(self) -> OrderedDictType[str, SQLQuery]:
    return OrderedDict()

  @property
  def all_using_query(self) -> SQLQuery:
    using_queries = self.using_queries
    if not using_queries:
      return SQLQuery('')
    using_aliases = using_queries.keys()
    newline = '\n'
    return SQLQuery(
      query=f'using {f",{newline}".join([f"{using_queries[a].query} as {a}" for a in using_aliases])}',
      substitution_parameters=tuple(p for a in using_aliases for p in using_queries[a].substitution_parameters)
    )

  def generate_query(self):
    assert self.condition_queries, 'Delete queries must have at least one condition'
    self.query = f'''
delete from {self.full_target_name}
{self.all_using_query.query}
{self.all_conditions_query.query}
    '''
    self.substitution_parameters = self.all_conditions_query.substitution_parameters + self.all_using_query.substitution_parameters

class CustomDeleteQuery(DeleteQuery):
  _using_queries: OrderedDictType[str, str]
  _condition_queries: List[SQLQuery]

  @property
  def using_queries(self) -> OrderedDictType[str, SQLQuery]:
    return self._using_queries

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return self._condition_queries

  def __init__(self, full_target_name: str, condition_queries: List[SQLQuery], using_queries: Dict[str, SQLQuery]={}):
    self._condition_queries = condition_queries
    self._using_queries = OrderedDict(using_queries.items())
    super().__init__(full_target_name=full_target_name)


class UpdateQuery(ConditionalQuery):
  full_target_name: str

  def __init__(self, full_target_name: str):
    self.full_target_name = full_target_name
    super().__init__()

  @property
  def set_queries(self) -> List[SQLQuery]:
    return []

  @property
  def all_sets_query(self) -> SQLQuery:
    assert self.set_queries, 'Update queries must have at least one set query'
    return SQLQuery(
      query='set ' + ', '.join([s.query for s in self.set_queries]),
      substitution_parameters=tuple(p for s in self.set_queries for p in s.substitution_parameters)
    )

  @property
  def from_queries(self) -> OrderedDictType[str, SQLQuery]:
    return OrderedDict()

  @property
  def all_from_query(self) -> SQLQuery:
    if not self.from_queries:
      return SQLQuery('')
    return SQLQuery(
      query='from ' + ', '.join([f'{f.query} as {a}' for a, f in self.from_queries.items()]),
      substitution_parameters=tuple(p for _, f in self.from_queries.items() for p in f.substitution_parameters)
    )

  def generate_query(self):
    self.query = f'''
update {self.full_target_name}
{self.all_sets_query.query}
{self.all_from_query.query}
{self.all_conditions_query.query}
    '''
    self.substitution_parameters = (
      *self.all_sets_query.substitution_parameters ,
      *self.all_from_query.substitution_parameters,
      *self.all_conditions_query.substitution_parameters,
    )

class CoalesceComparisonQuery(GeneratedQuery):
  leftQuery: SQLQuery
  rightQuery: SQLQuery
  nullPlaceholder: any
  operator: str

  def __init__(self, leftQuery: SQLQuery, rightQuery: SQLQuery, nullPlaceholder: any, operator: str='='):
    self.leftQuery = leftQuery
    self.rightQuery = rightQuery
    self.nullPlaceholder = nullPlaceholder
    self.operator = operator
    super().__init__()

  def generate_query(self):
    self.query = f'coalesce({self.leftQuery.query}, %s) {self.operator} coalesce({self.rightQuery.query}, %s)'
    self.substitution_parameters = (
      *self.leftQuery.substitution_parameters,
      self.nullPlaceholder,
      *self.rightQuery.substitution_parameters,
      self.nullPlaceholder,
    )

class LatestSelectQuery(SelectQuery):
  date_column: str
  value_column: str
  filter_column: str
  target_columns: List[str]
  date_aggregator: str
  value_aggregator: str
  exclude_values: List[any]
  value_query: Optional[SQLQuery]
  _condition_queries: List[SQLQuery]

  def __init__(self, full_source_name: str, date_column: str, value_column: str, filter_column: Optional[str]=None, target_columns: List[str]=[], date_aggregator: str='max', value_aggregator: str='max', exclude_values: List[any]=[], value_query: Optional[SQLQuery]=None, condition_queries: List[SQLQuery]=[]):
    self._full_source_name = full_source_name
    self.date_column = date_column
    self.value_column = value_column
    self.filter_column = filter_column if filter_column is not None else value_column
    self.target_columns = target_columns
    self.date_aggregator = date_aggregator
    self.value_aggregator = value_aggregator
    self.exclude_values = exclude_values
    self.value_query = value_query
    self._condition_queries = condition_queries
    
    super().__init__()

  @property
  def full_source_name(self) -> str:
    return self._full_source_name

  @property
  def source_columns(self) -> List[str]:
    return [
      *self.target_columns,
      self.value_column,
    ]

  @property
  def value_expression_query(self) -> SQLQuery:
    return self.value_query if self.value_query is not None else SQLQuery(f'"{self.value_column}"')

  @property
  def source_column_expressions(self) -> Dict[str, SQLQuery]:
    value_query = self.value_expression_query
    return {
      self.value_column: SQLQuery(
        query=f'{self.value_aggregator}({value_query.query})',
        substitution_parameters=value_query.substitution_parameters
      ),
    }

  def generate_query(self):
    target_columns_select = ', '.join([f'"{c}"' for c in self.target_columns])
    join = f'''
left join {self.full_source_name}
using ({target_columns_select}, "{self.date_column}")
    ''' if self.value_column != self.date_column else ''

    self.query = f'''
with d as (
  select {target_columns_select}, {self.date_aggregator}("{self.date_column}") as "{self.date_column}"
  from {self.full_source_name}
  {self.all_conditions_query.query}
  group by {target_columns_select}
)
select {self.all_columns_query.query}
from d
{join}
group by {target_columns_select}
    '''
    self.substitution_parameters = tuple([
      *self.all_conditions_query.substitution_parameters,
      *self.all_columns_query.substitution_parameters,
    ])

  @property
  def condition_queries(self) -> List[SQLQuery]:
    queries = [
      SQLQuery(f'{self.filter_column} != %s', (v,))
      for v in self.exclude_values if v is not None
    ]
    if None in self.exclude_values:
      queries.append(SQLQuery(f'{self.filter_column} is not null'))
    return [
      *self._condition_queries,
      *queries,
    ]

class LatestUpdateQuery(UpdateQuery):
  latest_select_query: LatestSelectQuery
  source_to_target_columns: Dict[str, str]
  target_condition_queries: List[SQLQuery]
  target_to_coalesce_placeholders: Dict[str, any]
  source_alias: str

  def __init__(self, full_target_name: str, latest_select_query: LatestSelectQuery, source_to_target_columns: Dict[str, str]={}, target_condition_queries: List[SQLQuery]=[], target_to_coalesce_placeholders: Dict[str, any]={}, source_alias='s'):
    self.latest_select_query = latest_select_query
    self.source_to_target_columns = source_to_target_columns
    self.target_condition_queries = target_condition_queries
    self.target_to_coalesce_placeholders = target_to_coalesce_placeholders
    self.source_alias = source_alias
    super().__init__(full_target_name=full_target_name)

  def target_for_source_column(self, source_column) -> str:
    return self.source_to_target_columns[source_column] if source_column in self.source_to_target_columns else source_column

  @property
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(f'"{self.target_for_source_column(self.latest_select_query.value_column)}" = {self.source_alias}."{self.latest_select_query.value_column}"')
    ]

  @property
  def from_queries(self) -> OrderedDictType[str, SQLQuery]:
    return OrderedDict([
      (self.source_alias, SQLQuery(f'({self.latest_select_query.query})', self.latest_select_query.substitution_parameters)),
    ])

  @property
  def condition_queries(self) -> List[SQLQuery]:
    return [
      *self.target_condition_queries,
      *[
        CoalesceComparisonQuery(
          leftQuery=SQLQuery(f'{self.full_target_name}."{self.target_for_source_column(t)}"'),
          rightQuery=SQLQuery(f'{self.source_alias}."{t}"'),
          nullPlaceholder=self.target_to_coalesce_placeholders[t]
        ) if t in self.target_to_coalesce_placeholders else SQLQuery(f'{self.full_target_name}."{self.target_for_source_column(t)}" = {self.source_alias}."{t}"')
        for t in self.latest_select_query.target_columns
      ],
    ]

class EditUpdateQuery(UpdateQuery):
  case_sensitive: bool
  match: Dict[str, Optional[str]]
  replace: Dict[str, Optional[any]]

  def __init__(self, full_target_name: str, match: Dict[str, Optional[str]], replace: Dict[str, any], case_sensitive: bool):
    self.match = match
    self.replace = replace
    super().__init__(full_target_name=full_target_name)

  @property
  def set_queries(self) -> List[SQLQuery]:
    return [
      SQLQuery(query=f'"{c}" = %s', substitution_parameters=(self.replace[c]))
      for c in sorted(self.replace.keys())
    ]

  @property
  def condition_queries(self) -> List[SQLQuery]:
    condition_columns = list(filter(lambda c: self.match[c] or self.match[c] is None, sorted(self.match.keys())))
    if self.case_sensitive:
      return [
        SQLQuery(query=f'"{c}"::character varying ~ %s', substitution_parameters=(self.match[c])) if self.match[c] is not None else SQLQuery(f'"{c}" is null')
        for c in condition_columns
      ]
    else:
      return [
        SQLQuery(query=f'lower("{c}"::character varying) ~ %s', substitution_parameters=(self.match[c].lower())) if self.match[c] is not None else SQLQuery(f'"{c}" is null')
        for c in condition_columns
      ]

# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

class JSONStringQuery(GeneratedQuery):
  expression: SQLQuery

  def __init__(self, expression: SQLQuery):
    self.expression = expression
    super().__init__()

  def generate_query(self):
    self.query = f"'\"' || replace({self.expression.query}, '\"', '\\\\\"') || '\"'"
    self.substitution_parameters = self.expression.substitution_parameters

class JSONArrayQuery(GeneratedQuery):
  expressions: List[SQLQuery]
  collection_space: str
  item_spacing: str

  def __init__(self, expressions: List[SQLQuery], collection_space: str='', item_space: str=''):
    self.expressions = expressions
    self.collection_space = collection_space
    self.item_space = item_space
    super().__init__()

  def generate_query(self):
    self.query = f"'[{self.collection_space}' || " + f"',{self.item_space}' || ".join(f'{e.query} || ' for e in self.expressions)  + f"'{self.collection_space}]'"
    self.substitution_parameters = (p for e in self.expressions for p in e.substitution_parameters)