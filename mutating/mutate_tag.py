import tasks

from .base import ReportMutator
from io_almacen.tag import NameTagsProcessor, TagUpdateMode, TagParserModel, TagParserNotFoundError
from io_almacen.channel import channel_entity_url
from config import configure

class ParseTagsMutator(ReportMutator[tasks.report_task]):
  specifier: tasks.ParseTagsMutationSpecifier

  def __init__(self, task: tasks.FetchCampaignReportTask, specifier: tasks.ParseTagsMutationSpecifier):
    super().__init__(task=task)
    self.specifier = specifier

  def mutate(self) -> str:
    if not configure['enable_tagging']:
      return 'Tagging disabled by configuration'
    if self.specifier.parser is None:
      return 'No parser specified'
    report = self.task.report
    if report is None or report.empty:
      return 'No entity names to parse in empty report'
    id_name_report = report.groupby(self.specifier.id_column).agg({self.specifier.name_column: 'max'})
    id_name_report.dropna(inplace=True)
    id_name_report.reset_index(inplace=True)
    urls_to_names = dict(zip(
      (channel_entity_url(
        channel=self.specifier.channel,
        entity=self.specifier.entity,
        entity_id=i
      ) for i in id_name_report[self.specifier.id_column]),
      id_name_report[self.specifier.name_column]
    ))
    if not urls_to_names:
      return 'No entity names to parse'
    processor = NameTagsProcessor(schema=self.task.report_table_schema)
    try:
      tags = processor.run(
        urls_to_names=urls_to_names,
        parser_url=f'{TagParserModel.maps_url_prefix}{self.specifier.parser}',
        update_mode=TagUpdateMode.url.value
      )
    except TagParserNotFoundError as e:
      return f'Tag parser not found {e.parser_url}'
    return f'Parsed {len(tags)} {self.specifier.entity}s and applied {sum(len(v) for v in tags.values())} total tags to {sum(1 for v in tags.values() if v)} {self.specifier.entity}s.'
