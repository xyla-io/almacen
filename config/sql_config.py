sql_config = {
  'default': {
    'user': 'USER',
    'password': 'PASSWORD',
    'schema': 'redshift+psycopg2',
    'host': 'HOST',
    'port': 5439,
    'database': 'DATABASE',
    'connector_options': {
      'sslmode': 'require',
      'aws_s3_access_key_id': 'KEY',
      'aws_s3_secret_access_key': 'SECRETKEY',
      's3_bucket': 'BUCKET',
      's3_bucket_region': 'us-east-2',
      's3_bucket_directory': 'DIRECTORY',
    },
  }, 
}
