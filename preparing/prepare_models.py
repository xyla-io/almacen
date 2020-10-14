from enum import Enum
from data_layer import SQLQuery
from typing import List, Optional
from abc import abstractproperty

class PrepareSQLTarget(Enum):
  settings = 'settings'
  schemas = 'schemas'
  tables = 'tables'
  functions = 'functions'
  materialized_views = 'materialized_views'
  views = 'views'
  procedures = 'procedures'

class SQLObjectType(Enum):
  schema = 'schema'
  table = 'table'
  matrialized_view = 'materialized_view'
  view = 'view'
  function = 'function'
  procedure = 'procedure'

class SQLObject:
  name: str
  schema: Optional[str]
  read_only_groups: List[str]
  substitution_parameters: any

  def __init__(self, name: str, schema: Optional[str]=None, read_only_groups: List[str]=[], substitution_parameters: any=()):
    self.name = name
    self.schema = schema
    self.read_only_groups = read_only_groups
    self.substitution_parameters=substitution_parameters

  @property
  def full_name(self) -> str:
    if self.schema is not None:
      return f'{self.schema}.{self.name}'
    else:
      return self.name

  @abstractproperty
  def object_type(self) -> SQLObjectType:
    pass

  @abstractproperty
  def existance_query(self) -> SQLQuery:
    pass

  @abstractproperty
  def drop_query(self) -> SQLQuery:
    pass

  @abstractproperty
  def read_only_grant_queries(self) -> List[SQLQuery]:
    pass

class SQLSchema(SQLObject):
  @property
  def object_type(self) -> SQLObjectType:
    return SQLObjectType.schema

  @property
  def existance_query(self) -> SQLQuery:
    return SQLQuery(
      query = """
SELECT EXISTS (
  SELECT 1
  FROM information_schema.schemata 
  WHERE schema_name = %s
);
    """,
      substitution_parameters=(self.name,)
    )

  @property
  def drop_query(self) -> SQLQuery:
    return SQLQuery(f'DROP SCHEMA IF EXISTS {self.name} CASCADE;')
  
  @property
  def read_only_grant_queries(self) -> List[SQLQuery]:
    return [SQLQuery(f'GRANT USAGE ON SCHEMA {self.name} TO GROUP {g};') for g in self.read_only_groups]

class SQLTable(SQLObject):
  empty_as_null: bool

  def __init__(self, name: str, schema: Optional[str]=None, read_only_groups: List[str]=[], substitution_parameters: any=(), empty_as_null: bool=False):
    super().__init__(name=name, schema=schema, read_only_groups=read_only_groups, substitution_parameters=substitution_parameters)
    self.empty_as_null = empty_as_null

  @property
  def object_type(self) -> SQLObjectType:
    return SQLObjectType.table

  @property
  def existance_query(self) -> SQLQuery:
    return SQLQuery(
      query = """
SELECT EXISTS (
  SELECT 1
  FROM information_schema.tables 
  WHERE table_schema = %s
  AND table_name = %s
);
    """,
      substitution_parameters=(self.schema, self.name)
    )

  @property
  def drop_query(self) -> SQLQuery:
    return SQLQuery(f'DROP TABLE IF EXISTS {self.full_name} CASCADE;')
  
  @property
  def read_only_grant_queries(self) -> List[SQLQuery]:
    return [SQLQuery(f'GRANT SELECT ON TABLE {self.full_name} TO GROUP {g};') for g in self.read_only_groups]

  def unload_query(self, destination: str, authorization_query: SQLQuery) -> SQLQuery:
    return SQLQuery(
      query = f'''
unload (%s)
to %s
{authorization_query.query}
escape
manifest verbose
allowoverwrite
header;
    ''',
      substitution_parameters=(
        f'select * from {self.full_name}', 
        destination,
        *authorization_query.substitution_parameters
      )
    )

  def load_query(self, source: str, authorization_query: SQLQuery, columns: Optional[List[str]]=None) -> SQLQuery:
    assert columns or columns is None
    quote = '"'
    columns_query = SQLQuery(f' ({", ".join(f"{quote}{c}{quote}" for c in columns)})') if columns else SQLQuery('')
    load_options = 'emptyasnull' if self.empty_as_null else ''
    return SQLQuery(
      query = f'''
copy {self.full_name}{columns_query.query}
from %s
{authorization_query.query}
{load_options}
escape
ignoreheader 1
manifest;
    ''',
      substitution_parameters=columns_query.substitution_parameters + (source,) + authorization_query.substitution_parameters
    )

class SQLMaterializedView(SQLTable):
  @property
  def object_type(self) -> SQLObjectType:
    return SQLObjectType.matrialized_view

  @property
  def drop_query(self) -> SQLQuery:
    return SQLQuery(f'DROP MATERIALIZED VIEW IF EXISTS {self.full_name};')
  

class SQLView(SQLObject):
  @property
  def object_type(self) -> SQLObjectType:
    return SQLObjectType.view

  @property
  def existance_query(self) -> SQLQuery:
    return SQLQuery(
      query = """
SELECT EXISTS (
  SELECT 1
  FROM information_schema.tables 
  WHERE table_schema = %s
  AND table_name = %s
);
    """,
      substitution_parameters=(self.schema, self.name)
    )

  @property
  def drop_query(self) -> SQLQuery:
    return SQLQuery(f'DROP VIEW IF EXISTS {self.full_name} CASCADE;')
  
  @property
  def read_only_grant_queries(self) -> List[SQLQuery]:
    return [SQLQuery(f'GRANT SELECT ON TABLE {self.full_name} TO GROUP {g};') for g in self.read_only_groups]

class SQLExecutable(SQLObject):
  arguments: Optional[str]

  def __init__(self, name: str, arguments: Optional[str]=None, schema: Optional[str]=None, read_only_groups: List[str]=[], substitution_parameters: any=()):
    super().__init__(name=name, schema=schema, read_only_groups=read_only_groups, substitution_parameters=substitution_parameters)
    self.arguments = arguments

  @property
  def existance_query(self) -> SQLQuery:
    # This query does not include the parameters and assumes that the function name and schema name are sufficient identifiers.
    # The pg_proc.proargtypes column contains argument type data as an oid vector, but we have not figured out how to use it.
    return SQLQuery(
      query = """
SELECT EXISTS (
  SELECT 1
  FROM pg_proc p 
  JOIN pg_namespace n 
  ON p.pronamespace = n.oid
  WHERE n.nspname::character varying = %s
  AND p.proname::character varying = %s
);
    """,
      substitution_parameters=(self.schema, self.name)
    )

class SQLFunction(SQLExecutable):
  @property
  def object_type(self) -> SQLObjectType:
    return SQLObjectType.function

  @property
  def drop_query(self) -> SQLQuery:
    arguments = f' ({self.arguments})' if self.arguments is not None else ' ()'
    return SQLQuery(f'DROP FUNCTION {self.full_name}{arguments} CASCADE;')
  
  @property
  def read_only_grant_queries(self) -> List[SQLQuery]:
    arguments = f' ({self.arguments})' if self.arguments is not None else ' ()'
    return [SQLQuery(f'GRANT EXECUTE ON FUNCTION {self.full_name}{arguments} TO GROUP {g};') for g in self.read_only_groups]

class SQLProcedure(SQLExecutable):
  @property
  def object_type(self) -> SQLObjectType:
    return SQLObjectType.procedure

  @property
  def drop_query(self) -> SQLQuery:
    arguments = f' ({self.arguments})' if self.arguments is not None else ' ()'
    return SQLQuery(f'DROP PROCEDURE {self.full_name}{arguments};')
  
  @property
  def read_only_grant_queries(self) -> List[SQLQuery]:
    arguments = f' ({self.arguments})' if self.arguments is not None else ''
    return [SQLQuery(f'GRANT EXECUTE ON PROCEDURE {self.full_name}{arguments} TO GROUP {g};') for g in self.read_only_groups]
