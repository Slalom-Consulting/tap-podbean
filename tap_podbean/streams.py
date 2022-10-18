"""Stream type classes for tap-podbean."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk.helpers.jsonpath import extract_jsonpath
import requests
from datetime import datetime, date
import re
import csv

from tap_podbean.client import PodbeanStream


SCHEMAS_DIR = Path(__file__).parent / Path('./schemas')

def get_schema_fp(file_name) -> str:
    return f'{SCHEMAS_DIR}/{file_name}.json'

class PrivateMembersStream(PodbeanStream):
    name = 'private_members'
    path = '/v1/privateMembers'
    records_jsonpath = '$.private_members[*]'
    primary_keys = ['email']
    replication_key = None
    schema_filepath = get_schema_fp('private_members')

class EpisodesStream(PodbeanStream):
    name = 'episodes'
    path = '/v1/episodes'
    records_jsonpath = '$.episodes[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = get_schema_fp('episodes')

class PodcastsStream(PodbeanStream):
    name = 'podcasts'
    path = '/v1/podcasts'
    records_jsonpath = '$.podcasts[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = get_schema_fp('podcasts')

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        return {
            'podcast_id': record['id'],
            'year': 2021 #TODO: fix year in config as partition
        }
class _PodbeanReportCsvStream(PodbeanStream):
    """Class for csv report streams"""
    primary_keys = [None]
    replication_key = None
    date_key = None
    records_jsonpath = '$.download_urls'
    parent_stream_type = PodcastsStream
    #partitions = {'years': [2022]}

    @property
    def start_date(self):
        return datetime.strptime(self.config.get('start_date'),'%Y-%m-%dT%H:%M:%S')

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse CSVs obtained from urls in the response and return iterator of the CSV records."""
        def filter_for_stream(val):
            """Reduce excess csv downloads"""
            pattern = r'^\d{4}-\d{1,2}$'
            
            if re.match(pattern, val):
                report_month = datetime.strptime(val,'%Y-%m').date()
                start_month = date(self.start_date.year, self.start_date.month, 1)
                
                if report_month >= start_month:
                    return True

        def extract_url(val):
            if isinstance(val, list):
                return val[0]
            
            return val

        iter_records = (r for r in extract_jsonpath(self.records_jsonpath, input=response.json()))
        iter_urls = (extract_url(v) for r in iter_records for k,v in r.items() if filter_for_stream(k) and v)

        for url in iter_urls:
            with requests.get(url, stream=True) as r:
                f = (line.decode('utf-8-sig') for line in r.iter_lines())
                reader = csv.DictReader(f, delimiter=',')

                for row in reader:
                    yield row

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Add context to and filter row"""
        record_date_text = row.get(self.date_key).lstrip("'")

        #cleans up excel text char in date
        #if record_date_text[0] == "'":
        #    record_date_text = record_date_text.lstrip("'")
        #    row[self.date_key] = record_date_text

        record_date = datetime.strptime(record_date_text,'%Y-%m-%d %H:%M:%S')

        if record_date >= self.start_date:
            id = context.get('podcast_id')
            return {'podcast_id': id, **row}

class PodcastDownloadReportsStream(_PodbeanReportCsvStream):
    name = 'podcast_download_reports'
    path = '/v1/analytics/podcastReports'
    schema_filepath = get_schema_fp('podcast_download_reports')
    date_key = 'Time(GMT)'

class PodcastEngagementReportsStream(_PodbeanReportCsvStream):
    name = 'podcast_engagement_reports'
    path = '/v1/analytics/podcastEngagementReports'
    schema_filepath = get_schema_fp('podcast_engagement_reports')
    date_key = 'Time(GMT)'

class PodcastAnalyticReportsStream(PodbeanStream):
    name = 'podcast_analytic_reports'
    path = '/v1/analytics/podcastAnalyticReports'
    schema_filepath = get_schema_fp('podcast_engagement_reports')
    parent_stream_type = PodcastsStream
    types = ('followers','likes','comments','total_episode_length')