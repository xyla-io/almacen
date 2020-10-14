from config import company_configs, company_config_values

def test_company_configs():
  assert company_configs

def test_company_config_values():
  values = company_config_values(company_configs)
  assert values
