import pandas as pd
import tasks
import pdb

from typing import TypeVar, Generic, Dict, Optional, List
from datetime import datetime, timedelta

T = TypeVar(tasks.ReportTask)
class ReportProcessor(Generic[T]):
  task: T

  def __init__(self, task: T):
    self.task = task

  @property
  def added_columns(self) -> Dict[str, any]:
    return {
      'company_display_name': self.task.company_display_name
    }

  @property
  def null_column_defaults(self) -> Dict[str, any]:
    return {}

  def add_product_canonical_columns(self, report: pd.DataFrame):
    for product in self.task.task_set.products:
      report.loc[report['product'] == product.identifier, 'product_name'] = product.display_name
      for os, platform_id in product.platform_ids.items():
        report.loc[report.product_id == str(platform_id), ['product', 'product_name', 'product_platform', 'product_os']] = [product.identifier, product.display_name, 'mobile', os]

  def process(self) -> Optional[any]:
    # transform date column values to datetime values
    report = self.task.report
    model = self.task.report_table_model
    if model.date_format_string is not None:
      report[model.date_column_name] = report[model.date_column_name].apply(lambda d: datetime.strptime(d, model.date_format_string) if isinstance(d, str) else d)

    # replace parentheses with brackets in column names
    report.rename(lambda col_name: col_name.replace('(', '[').replace(')', ']'), axis='columns', inplace=True)

    # add supplementary columns provided by context
    for (column_name, column_value) in self.added_columns.items():
      report[column_name] = column_value

    for (column_name, column_value) in self.null_column_defaults.items():
      report.loc[report[column_name].isnull(), column_name] = column_value

    # set crystallization status
    uncrystallized_start_date = datetime.combine(self.task.run_date - self.task.crystallization_time + timedelta(days=1), datetime.min.time())
    report[model.crystallized_column_name] = report[model.date_column_name].apply(lambda d: d < uncrystallized_start_date)

    # set fetch date
    report['fetch_date'] = self.task.run_date

class CurrencyExchangeReportProcessor(ReportProcessor[T]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {}

class VoidReportProcessor(ReportProcessor[tasks.ReportTask]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {}

  def process(self):
    pass

class ReportEditor:
  class Edit:
    case_sensitive: bool
    match: Dict[str, Optional[str]]
    replace: Dict[str, any]

    def __init__(self, configuration: Dict[str, any]):
      self.case_sensitive = configuration['case_sensitive']
      self.match = {**configuration['match']}
      self.replace = {**configuration['replace']}

    def edit_report(self, report: pd.DataFrame):
      if report.empty or not self.replace:
        return
      matched_report = report
      for column, pattern in [(k, self.match[k]) for k in sorted(self.match.keys())]:
        if column not in matched_report:
          matched_report[column] = None
        if pattern is not None:
          matched_report = matched_report[matched_report[column].str.match(pattern, case=self.case_sensitive, na=False)]
        else:
          matched_report = matched_report[pd.isna(matched_report[column])]
        if matched_report.empty:
          return
      
      replace_items = [(k, self.replace[k]) for k in sorted(self.replace.keys())]
      for replace_item in replace_items:
        if replace_item[0] not in report:
          report[replace_item[0]] = None
      report.loc[matched_report.index, [i[0] for i in replace_items]] = [i[1] for i in replace_items]

  edits: List[Edit]

  def __init__(self, configuration: List[Dict[str, any]]):
    self.edits = [ReportEditor.Edit(c) for c in configuration]

  def edit_report(self, report: pd.DataFrame):
    # The Edits use the report index to match rows and repeated index entries will cause incorrect matching
    report.reset_index(drop=True, inplace=True)
    report_columns = list(report.columns)
    for edit in self.edits:
      edit.edit_report(report=report)
    added_columns = list(filter(lambda c: c not in report_columns, report.columns))
    if added_columns:
      report.drop(columns=added_columns, inplace=True)

class EditReportProcessor(ReportProcessor[tasks.ReportTask]):
  @property
  def added_columns(self) -> Dict[str, any]:
    return {}

  def process(self):
    if not self.task.edits:
      return
    editor = ReportEditor(configuration=self.task.edits)
    editor.edit_report(report=self.task.report)
    return f'Applied edits: {self.task.edits}'
