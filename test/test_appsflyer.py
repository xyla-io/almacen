import unittest
import pandas as pd
import os

from salem import AppsFlyerAPI, AppsFlyerReporter
from datetime import datetime
from typing import Dict

class Test_AppsFlyer(unittest.TestCase):
  def setUp(self):
    api = AppsFlyerAPI(api_key='APIKEY')
    self.reporter = AppsFlyerReporter(api=api)

  def test_purchase_event_data_stability(self):
    """
    Test whether and how much event reports for a fixed date change over time.
    """
    csv_path = os.path.join(os.path.dirname(__file__), 'test_purchase_event_data_stability.csv')
    try:
      df = pd.read_csv(csv_path)
    except:
      df = pd.DataFrame()
    
    event_name = 'af_purchase'

    android_record = self.collect_event_count_record(date=datetime(2018, 1, 1), app_id='APPID', event_name=event_name)    
    df = df.append([android_record])

    apple_record = self.collect_event_count_record(date=datetime(2018, 1, 1), app_id='APPID', event_name=event_name)    
    df = df.append([apple_record])

    df.to_csv(csv_path, index=False)

  def collect_event_count_record(self, date: datetime, app_id: str, event_name: str) -> Dict[str, any]:
    self.reporter.api.app_id = app_id
    report = self.reporter.get_events_report(start_date=date, end_date=date, event_names=[event_name])
    self.reporter.api.app_id = None

    return {
      'app_id': app_id,
      'event_name': event_name,
      'report_date': date,
      'fetch_date': datetime.utcnow(),
      'row_count': report.shape[0],
    }
    
