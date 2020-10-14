import pytest
import models

from config import sql_config
from preparing import SQLPreparer

@pytest.fixture
def preparer() -> SQLPreparer:
  models.SQL.Layer.configure_connection(sql_config['default'])
  return SQLPreparer(sql_layer=models.SQL.Layer(echo=True), schema_name='demo')

def test_manifest(preparer: SQLPreparer):
  manifests = preparer.get_sql_objects_manifests(objects=[t for t in preparer.tables if t.name == 'fetch_facebook_campaigns'], source_bucket='BUCKET', source_path='unload/production')
  print(manifests)
  return manifests