import os
import pdb
import models
import boto3
import json

from pathlib import Path
from typing import List, Optional, Dict
from enum import Enum
from data_layer import SQLQuery, SQLLayer
from .prepare_models import SQLObject, SQLExecutable, SQLSchema, SQLTable, SQLMaterializedView, SQLView, SQLFunction, SQLProcedure

class Status(Enum):
  info = '\033[94m'
  normal = '\033[92m'
  warning = '\033[93m'
  error = '\033[91m'

  def text(self, text: str):
    return '{}{}\033[0m'.format(self.value, text)

class SQLPreparer:
  sql_layer: Optional[SQLLayer]
  schema_name: Optional[str]

  def __init__(self, sql_layer: Optional[SQLLayer]=None, schema_name: Optional[str]=None):
    self.sql_layer = sql_layer
    self.schema_name = schema_name

  @property
  def schemas(self) -> List[SQLSchema]:
    return [
      SQLSchema(self.schema_name, read_only_groups=['all_client_readers'])
    ]

  @property
  def tables(self) -> List[SQLTable]:
    return [
      SQLTable('entity_ad_materialized', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('entity_adset_materialized', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('entity_campaign_materialized', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_adjust_cohorts_measures_daily', schema=self.schema_name, read_only_groups=['all_client_readers']),   
      SQLTable('fetch_adjust_cohorts_measures_monthly', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_adjust_cohorts_measures_weekly', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_adjust_deliverables', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_adjust_events', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_apple_search_ads_campaigns', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_apple_search_ads_adgroups', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_apple_search_ads_keywords', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_apple_search_ads_creative_sets', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_apple_search_ads_searchterms', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_appsflyer_custom_events', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_appsflyer_install_events', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_appsflyer_purchase_events', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_appsflyer_data_locker', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_currency_exchange_rates', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_facebook_ads', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_facebook_adsets', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_facebook_campaigns', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_google_ads_ad_conversion_actions', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('fetch_google_ads_assets', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('fetch_google_ads_ads', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('fetch_google_ads_ad_assets', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('fetch_google_ads_ad_groups', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('fetch_google_ads_campaigns', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('fetch_snapchat_ads', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_snapchat_adsquads', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_snapchat_campaigns', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('fetch_tiktok_ads', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('fetch_tiktok_adgroups', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('fetch_tiktok_campaigns', schema=self.schema_name, read_only_groups=['all_client_readers'], empty_as_null=True),
      SQLTable('scrape_google_uac_creatives', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('tag_ads', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('tag_adsets', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('tag_campaigns', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('track_task_history', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('view_windows', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('performance_cube_unfiltered', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('performance_cube_filtered', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('performance_goals', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('tags', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('url_numbers', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLTable('maps', schema=self.schema_name, read_only_groups=['all_client_readers']),
    ]

  @property
  def materialized_views(self) -> List[SQLTable]:
    return [
      SQLMaterializedView('standard_tags', schema=self.schema_name, read_only_groups=['all_client_readers']),
    ]

  @property
  def views(self) -> List[SQLView]:
    return [
      SQLView('last_task_completion', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('source_ranges', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_apple_search_ads_keyword', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_apple_search_ads_creative_set', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_apple_search_ads_adgroup', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_apple_search_ads_campaign', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_facebook_ad', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_facebook_adset', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_facebook_campaign', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_google_ads_ad', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_google_ads_ad_group', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_google_ads_campaign', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_google_ads_conversion_action', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_snapchat_ad', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_snapchat_adsquad', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_snapchat_campaign', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_tiktok_ad', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_tiktok_adgroup', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_tiktok_campaign', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_adjust_daily', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_adjust_weekly', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_adjust_monthly', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('entity_campaign', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('entity_adset', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('entity_ad', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_by_campaign', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_by_adset', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_by_ad', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_by_keyword', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_by_searchterm', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_basic', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLView('cube_advanced', schema=self.schema_name, read_only_groups=['all_client_readers']),
    ]

  @property
  def functions(self) -> List[SQLFunction]:
    return [
      SQLFunction('monday_of_week', arguments='date', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLFunction('appsflyer_media_source_channel', arguments='character varying', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLFunction('adjust_network_channel', arguments='character varying', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLFunction('keyword_ad_id', arguments='character varying, character varying, character varying', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLFunction('uuid', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLFunction('try_int', arguments='s character varying', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLFunction('try_float', arguments='s character varying', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLFunction('channel_entity_url', arguments='character varying, character varying, character varying', schema=self.schema_name, read_only_groups=['all_client_readers']),
      SQLFunction('quote_json', arguments='character varying', schema=self.schema_name, read_only_groups=['all_client_readers']),
    ]

  @property
  def procedures(self) -> List[SQLProcedure]:
    return [
      SQLProcedure('number_urls', arguments='character varying, character varying, character varying', schema=self.schema_name),
      SQLProcedure('update_url_numbers', arguments='character varying, character varying, character varying, bool', schema=self.schema_name),
      SQLProcedure('update_all_url_numbers', arguments='bool', schema=self.schema_name),
      SQLProcedure('replace_standard_tags_view', schema=self.schema_name),
    ]

  @property
  def s3_authorization_query(self) -> str:
    return SQLQuery(
      query = f'access_key_id %s secret_access_key %s',
      substitution_parameters=(self.sql_layer.connection_options.connector_options['aws_s3_access_key_id'], self.sql_layer.connection_options.connector_options['aws_s3_secret_access_key'])
    )

  def configure_settings(self, user: Optional[str]=None, password: Optional[str]=None):
    pass

  def create_schemas(self, drop_if_exist: bool=False):
    schemas = self.sql_objects_that_do_not_exist(objects=self.schemas, drop_if_exist=drop_if_exist)
    self.sql_layer.connect()
    for schema in schemas:
      create_query = SQLQuery(f'CREATE SCHEMA {schema.name};')
      create_query.run(self.sql_layer)
    self.sql_layer.commit()
    self.sql_layer.disconnect()
    self.grant_privileges_on_sql_objects(self.schemas)

  def create_tables(self, drop_if_exist: bool=False, relations: Optional[List[str]]=None):
    target_tables = [r for r in self.tables if r.name in relations] if relations is not None else self.tables
    tables = self.sql_objects_that_do_not_exist(objects=target_tables, drop_if_exist=drop_if_exist)
    if tables:
      print(Status.normal.text(f'Creating {len(tables)} tables:\n  ' + '\n  '.join([t.full_name for t in tables])))
    self.create_sql_objects(tables)
    self.grant_privileges_on_sql_objects(target_tables)

  def create_materialized_views(self, drop_if_exist: bool=False, relations: Optional[List[str]]=None):
    target_materialized_views = [r for r in self.materialized_views if r.name in relations] if relations is not None else self.materialized_views
    materialized_views = self.sql_objects_that_do_not_exist(objects=target_materialized_views, drop_if_exist=drop_if_exist)
    if materialized_views:
      print(Status.normal.text(f'Creating {len(materialized_views)} materialized views:\n  ' + '\n  '.join([v.full_name for v in materialized_views])))
    self.create_sql_objects(materialized_views)
    self.grant_privileges_on_sql_objects(target_materialized_views)
    
  def create_views(self, drop_if_exist: bool=False, replace_if_exist: bool=True, relations: Optional[List[str]]=None):
    target_views = [r for r in self.views if r.name in relations] if relations is not None else self.views
    views = self.sql_objects_that_do_not_exist(objects=target_views, drop_if_exist=drop_if_exist)
    if views:
      print(Status.normal.text(f'Creating {len(views)} views:\n  ' + '\n  '.join([v.full_name for v in views])))
    if not replace_if_exist:
      target_views = views
    if len(target_views) > len(views):
      print(Status.info.text(f'Replacing {len(target_views) -  len(views)} views:\n  ' + '\n  '.join([v.full_name for v in target_views if v not in views])))
    self.create_sql_objects(target_views)
    self.grant_privileges_on_sql_objects(target_views)

  def create_functions(self, drop_if_exist: bool=False, replace_if_exist: bool=True, relations: Optional[List[str]]=None):
    target_functions = [r for r in self.functions if r.name in relations] if relations is not None else self.functions
    functions = self.sql_objects_that_do_not_exist(objects=target_functions, drop_if_exist=drop_if_exist)
    if functions:
      print(Status.normal.text(f'Creating {len(functions)} functions:\n  ' + '\n  '.join([f.full_name for f in functions])))
    if not replace_if_exist:
      target_functions = functions
    if len(target_functions) > len(functions):
      print(Status.info.text(f'Creating or replacing {len(target_functions) - len(functions)} functions:\n  ' + '\n  '.join([f.full_name for f in target_functions if f not in functions])))
    self.create_sql_objects(target_functions)
    self.grant_privileges_on_sql_objects(target_functions)

  def create_procedures(self, drop_if_exist: bool=True, replace_if_exist: bool=True, relations: Optional[List[str]]=None):
    target_procedures = [r for r in self.procedures if r.name in relations] if relations is not None else self.procedures
    procedures = self.sql_objects_that_do_not_exist(objects=target_procedures, drop_if_exist=drop_if_exist)
    if procedures:
      print(Status.normal.text(f'Creating {len(procedures)} procedures:\n  ' + '\n  '.join([p.full_name for p in procedures])))
    if not replace_if_exist:
      target_procedures = procedures
    if len(target_procedures) > len(procedures):
      print(Status.info.text(f'Creating or replacing {len(target_procedures) - len(procedures)} procedures:\n  ' + '\n  '.join([p.full_name for p in target_procedures if p not in procedures])))
    self.create_sql_objects(target_procedures)
    self.grant_privileges_on_sql_objects(target_procedures)

  def unload_tables(self, path: str, drop_if_exist: bool=False, relations: Optional[List[str]]=None):
    tables = [t for t in self.tables if t.name in relations] if relations is not None else self.tables
    destination = 's3://{}/{}'.format(self.sql_layer.connection_options.connector_options['s3_bucket'], path)
    if tables:
      print(Status.normal.text(f'Unloading {len(tables)} tables:\n  ' + '\n  '.join([t.full_name for t in tables])))
    self.unload_sql_objects(objects=tables, destination_directory=destination, authorization_query=self.s3_authorization_query)

  def load_tables(self, path: str, drop_if_exist: bool=False, relations: Optional[List[str]]=None):
    tables = [t for t in self.tables if t.name in relations] if relations is not None else self.tables
    if drop_if_exist:
      tables = self.sql_objects_that_do_not_exist(objects=tables, drop_if_exist=drop_if_exist)
      self.create_sql_objects(objects=tables)
      self.grant_privileges_on_sql_objects(tables)
    if tables:
      print(Status.normal.text(f'Loading {len(tables)} tables:\n  ' + '\n  '.join([t.full_name for t in tables])))
    self.load_sql_objects(objects=tables, source_bucket=self.sql_layer.connection_options.connector_options['s3_bucket'], source_path=path, authorization_query=self.s3_authorization_query)
  
  def sql_objects_that_do_not_exist(self, objects: List[SQLObject], drop_if_exist: bool=False) -> List[SQLObject]:
    non_extant_objects = []
    self.sql_layer.connect()

    for o in objects:
      object_exists = o.existance_query.run(sql_layer=self.sql_layer).fetchone()[0]

      if object_exists and drop_if_exist:
        o.drop_query.run(self.sql_layer)
        object_exists = False

      if not object_exists:
        non_extant_objects.append(o)

    self.sql_layer.commit()
    self.sql_layer.disconnect()
    return non_extant_objects

  def create_sql_objects(self, objects: List[SQLObject]):
    self.sql_layer.connect()

    for sql_object in objects:
      sql_file_name = 'create_' + sql_object.object_type.value + '_' + sql_object.name
      self.run_sql_file(file_name=sql_file_name, sql_layer=self.sql_layer, schema_name=sql_object.schema, substitution_parameters=sql_object.substitution_parameters)
    
    self.sql_layer.disconnect()

  def grant_privileges_on_sql_objects(self, objects: List[SQLExecutable]):
    self.sql_layer.connect()

    for sql_object in objects:
      for grant_query in sql_object.read_only_grant_queries:
        grant_query.run(self.sql_layer)

    self.sql_layer.commit()
    self.sql_layer.disconnect()

  def unload_sql_objects(self, objects: List[SQLTable], destination_directory: str, authorization_query: SQLQuery):
    self.sql_layer.connect()

    for sql_object in objects:
      destination = f'{destination_directory}/{sql_object.full_name}_'
      query = sql_object.unload_query(destination=destination, authorization_query=authorization_query)
      query.run(self.sql_layer)
    
    self.sql_layer.disconnect()

  def load_sql_objects(self, objects: List[SQLTable], source_bucket: str, source_path: str, authorization_query: SQLQuery):
    self.sql_layer.connect()
    manifests = self.get_sql_objects_manifests(objects=objects, source_bucket=source_bucket, source_path=source_path)
    objects_and_columns = list(map(lambda o, m: (o, [c['name'] for c in m['schema']['elements']]), objects, manifests))
    for sql_object, columns in objects_and_columns:
      source = f's3://{source_bucket}/{source_path}/{sql_object.full_name}_manifest'
      query = sql_object.load_query(source=source, authorization_query=authorization_query, columns=columns)
      query.run(self.sql_layer)
      self.sql_layer.commit()
    
    self.sql_layer.disconnect()

  def get_sql_objects_manifests(self, objects: List[SQLTable], source_bucket: str, source_path: str):
    connector_options = self.sql_layer.connection_options.connector_options
    s3 = boto3.resource(
      service_name='s3',
      region_name=connector_options['s3_bucket_region'],
      aws_access_key_id=connector_options['aws_s3_access_key_id'], 
      aws_secret_access_key=connector_options['aws_s3_secret_access_key']
    )
    results = [s3.meta.client.get_object(Bucket=connector_options['s3_bucket'], Key=f'{source_path}/{o.full_name}_manifest') for o in objects]
    manifests = [json.load(r['Body']) for r in results]
    return manifests

  def run_custom_sql_file(self, sql_file_path, substitution_parameters: any=(), template_parameters: Optional[Dict[str, str]]=None):
    sql = Path(sql_file_path).read_text()
    self.run_custom_sql(sql=sql, substitution_parameters=substitution_parameters, template_parameters=template_parameters)

  def run_custom_sql(self, sql, substitution_parameters: any=(), template_parameters: Optional[Dict[str, str]]=None):
    if template_parameters is None:
      template_parameters = {
        'SCHEMA': f'{self.schema_name}.' if self.schema_name is not None else '',
      }
    populated_sql = sql.format(**template_parameters) if template_parameters else sql
    self.sql_layer.connect()
    self.sql_layer.connection.autocommit = True
    self.sql_layer.query(populated_sql, substitution_parameters=substitution_parameters)
    self.sql_layer.disconnect()

  def run_sql_file(self, file_name: str, sql_layer: SQLLayer, schema_name: Optional[str]=None, substitution_parameters: any=()):
    sql = self.read_sql_file(file_name=file_name, schema_name=schema_name)
    sql_layer.query(sql, substitution_parameters=substitution_parameters)
    sql_layer.commit()  

  def read_sql_file(self, file_name: str, schema_name: Optional[str]=None) -> str:
    directory = os.path.dirname(__file__)
    path = os.path.join(directory, 'sql', file_name + '.sql')
    sql_text = Path(path).read_text()

    schema = f'{schema_name}.' if schema_name is not None else ''
    sql_text = sql_text.format(SCHEMA=schema)

    return sql_text