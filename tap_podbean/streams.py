"""Stream type classes for tap-podbean."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from requests import request
from singer_sdk import typing as th  # JSON schema typing helpers
import json
import requests
import re
import csv
import io

from tap_podbean.client import PodbeanStream
from tap_podbean.paginator import PodbeanPaginator


SCHEMAS_DIR = Path(__file__).parent / Path('./schemas')

def get_schema_fp(file_name) -> str:
    return f'{SCHEMAS_DIR}/{file_name}.json'


def csv_download(url):
    response = requests.get(url)
    text = response.iter_lines()
    reader = csv.reader(text, delimiter=',')

class PrivateMembersStream(PodbeanStream):
    """Define custom stream."""
    name = 'private_members'
    path = '/v1/privateMembers'
    records_jsonpath = '$.private_members[*]'
    primary_keys = ['email']
    replication_key = None
    schema_filepath = get_schema_fp('private_members')

class EpisodesStream(PodbeanStream):
    """Define custom stream."""
    name = 'episodes'
    path = '/v1/episodes'
    records_jsonpath = '$.episodes[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = get_schema_fp('episodes')



class PodcastsStream(PodbeanStream):
    """Define custom stream."""
    name = 'podcasts'
    path = '/v1/podcasts'
    records_jsonpath = '$.podcasts[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = get_schema_fp('podcasts')

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            'podcast_id': record['id'],
            'year': 2021
        }

class _PodbeanReportCsvStream(PodbeanStream):
    primary_keys = [None]
    replication_key = None
    records_jsonpath = '$.download_urls'
    parent_stream_type = PodcastsStream

    #@property
    #def schema(self) -> dict:
    #    if not self._schema:
    #        fp = get_schema_fp('report_download')
    #        with open(fp, 'r') as f:
    #            self._schema:dict = json.load(f)
#
    #    return self._schema


    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Clean up response and update schema
        
        Returns
            Dict of response records excluding empty records
        """
        #def pattern_match(val) -> bool:
        #    pattern_properties:dict = self._schema.get('patternProperties')
        #    
        #    for pattern in pattern_properties:
        #        schema = pattern_properties[pattern]
#
        #        if re.match(pattern, val):
        #            self._schema['properties'][val] = schema
        #            return True
#
        #def remove_empty(val) -> list:
        #    return [v for v in val if v]

        pattern = r'^\d{4}-\d{1,2}$'
        download_urls = [v for k,v in row.items() if re.match(pattern, k) and v]

        #TODO: clean up reports output

        report = []
        for url in download_urls:
            response = requests.get(url)
            reader = csv.DictReader(response.text.splitlines(), delimiter=',')
            data = [r for r in reader]
            report.append(data)
            
        out = {
            'podcast_id': context.get('podcast_id'),
            'report': report
        }

        return out


    #def get_child_context(self, record: dict, context: dict | None) -> dict:
    #    period = list(record.items())[0]
    #    return {
    #        'download_url': record[period][0],
    #       #TODO: test if podcast_id is still in context
    #    }


class PodcastDownloadReportsStream(_PodbeanReportCsvStream):
    """Define custom stream."""
    name = 'podcast_download_reports'
    path = '/v1/analytics/podcastReports'
    schema_filepath = get_schema_fp('podcast_download_reports')

class PodcastEngagementReportsStream(_PodbeanReportCsvStream):
    """Define custom stream."""
    name = 'podcast_engagement_reports'
    path = '/v1/analytics/podcastEngagementReports'
    schema_filepath = get_schema_fp('podcast_engagement_reports')
