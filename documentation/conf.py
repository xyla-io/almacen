import os
import sys

from datetime import datetime

sys.path.insert(0, os.path.abspath('..'))
extensions = [
  'sphinx.ext.autodoc',
  'sphinx.ext.autosummary',
]

project = u'Almacén'
author = u'Gregory Klein'
copyright = f'{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}, Xyla'
master_doc = 'documentation/index'
exclude_patterns = [
  'venv',
  'credentials',
  'config/company_configs',
  'config/local_*',
  'development_packages',
]
autosummary_generate = True
html_theme = 'groundwork'