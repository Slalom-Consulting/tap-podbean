"""Stream type classes for tap-podbean."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from singer_sdk.helpers.jsonpath import extract_jsonpath
import requests
import re
import csv

from tap_podbean.client import PodbeanStream


SCHEMAS_DIR = Path(__file__).parent / Path('./schemas')

def get_schema_fp(file_name) -> str:
    return f'{SCHEMAS_DIR}/{file_name}.json'

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
            'year': 2021 #TODO: fix year in config as partition
        }

class _PodbeanReportCsvStream(PodbeanStream):
    primary_keys = [None]
    replication_key = None
    records_jsonpath = '$.download_urls'
    parent_stream_type = PodcastsStream

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.
        Args:
            response: A raw `requests.Response`_ object.
        Yields:
            One item for every item found in the response.
        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response

        returns a dict of a row from a csv that is streamed from a url that comes from a list of urls that comes from a response
        """

        pattern = r'^\d{4}-\d{1,2}$'
        iter_records = (r for r in extract_jsonpath(self.records_jsonpath, input=response.json()))

        for record in iter_records:
            urls = [v for k,v in record.items() if re.match(pattern, k) and v]

            for url in urls:
                with requests.get(url, stream=True) as r:
                    f = (line.decode('utf-8-sig') for line in r.iter_lines())
                    reader = csv.DictReader(f, delimiter=',')
                    for row in reader:
                        yield row

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """adds in the podcast id to row"""
        id = {
            'podcast_id': context.get('podcast_id')
        }

        return {**id, **row}

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
