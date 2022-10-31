"""Stream type classes for tap-podbean."""

from typing import Any, Dict, Optional, List, Iterable
from tap_podbean.auth import PodbeanPartitionAuthenticator
from tap_podbean.client import PodbeanStream
from memoization import cached
from pathlib import Path

from datetime import datetime, date
from singer_sdk.helpers.jsonpath import extract_jsonpath
import csv
import json
import requests
import re

SCHEMAS_DIR = Path(__file__).parent / Path('./schemas')


class PrivateMembersStream(PodbeanStream):
    name = 'private_members'
    path = '/v1/privateMembers'
    records_jsonpath = '$.private_members[*]'
    primary_keys = ['email']
    replication_key = None
    schema_filepath = f'{SCHEMAS_DIR}/private_members.json'


class PodcastsStream(PodbeanStream):
    name = 'podcasts'
    path = '/v1/podcasts'
    records_jsonpath = '$.podcasts[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = f'{SCHEMAS_DIR}/podcasts.json'


class _PodcastPartitionStream(PodbeanStream):
    """Base class for podcast partitions"""
    @property
    @cached
    def authenticator(self) -> PodbeanPartitionAuthenticator:
        return PodbeanPartitionAuthenticator(self)


class EpisodesStream(_PodcastPartitionStream):
    name = 'episodes'
    path = '/v1/episodes'
    records_jsonpath = '$.episodes[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = f'{SCHEMAS_DIR}/episodes.json'

    @property
    def partitions(self) -> List[dict]:
        return [{'podcast_id':k} for k in self.authenticator.tokens.keys()]

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[int]
        ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        podcast_id = context.get('podcast_id')
        params['access_token'] = self.authenticator.tokens.get(podcast_id)
        return params


class _CsvStream(_PodcastPartitionStream):
    """Class for csv report streams"""
    primary_keys = [None]
    replication_key = None
    response_date_key = None
    records_jsonpath = '$.download_urls'

    @property
    def start_date(self) -> datetime:
        return datetime.strptime(self.config.get('start_date'), '%Y-%m-%dT%H:%M:%S')

    @property
    def partitions(self) -> List[dict]:
        podcast_ids = [p for p in self.authenticator.tokens.keys()]

        start_year = self.start_date.year
        current_year = datetime.utcnow().date().year
        years = [current_year]

        if start_year < current_year:
            year_rng = range(current_year + 1 - start_year)
            years = [start_year + y for y in year_rng]
    
        elif start_year > current_year:
            years = [start_year]
    
        def json_str(podcast_id, year) -> str:
            """Parameters for CSV Reports"""
            part = {
                'podcast_id': podcast_id,
                'year': year
            }
            return json.dumps(part)

        return [{'partition':json_str(p,y)} for p in podcast_ids for y in years]

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[int]
        ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        parts: dict = json.loads(context.get('partition'))
        podcast_id = parts.get('podcast_id')
        return {
            'access_token': self.authenticator.tokens.get(podcast_id),
            'podcast_id': podcast_id,
            'year': parts.get('year')
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse CSVs obtained from urls in the response and return iterator of the CSV records."""
        def filter_for_stream(val) -> bool:
            """Reduce excess csv downloads"""
            pattern = re.compile(r'^\d{4}-\d{1,2}$')
            
            if pattern.match(val):
                report_month = datetime.strptime(val,'%Y-%m').date()
                start_month = date(self.start_date.year, self.start_date.month, 1)
                return report_month >= start_month

        def extract_url(val) -> str:
            return val[0] if isinstance(val, list) else val

        iter_records = (r for r in extract_jsonpath(self.records_jsonpath, input=response.json()))
        iter_urls = (extract_url(v) for r in iter_records for k,v in r.items() if filter_for_stream(k) and v)

        for url in iter_urls:
            with requests.get(url, stream=True) as response:
                file = (line.decode('utf-8-sig') for line in response.iter_lines())
                reader = csv.DictReader(file, delimiter=',')

                for row in reader:
                    yield row

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Add context to and filter row"""
        record_date_text: str = row.get(self.response_date_key)
        record_date_text = record_date_text.lstrip("'")  # removes leading excel char
        record_date = datetime.strptime(record_date_text,'%Y-%m-%d %H:%M:%S')

        if record_date >= self.start_date:
            parts: dict = json.loads(context.get('partition'))
            id = parts.get('podcast_id')
            return {'podcast_id': id, **row}


class PodcastDownloadReportsStream(_CsvStream):
    name = 'podcast_download_reports'
    path = '/v1/analytics/podcastReports'
    schema_filepath = f'{SCHEMAS_DIR}/podcast_download_reports.json'
    response_date_key = 'Time(GMT)'


class PodcastEngagementReportsStream(_CsvStream):
    name = 'podcast_engagement_reports'
    path = '/v1/analytics/podcastEngagementReports'
    schema_filepath = f'{SCHEMAS_DIR}/podcast_engagement_reports.json'
    response_date_key = 'Time(GMT)'


class NetworkAnalyticReportsStream(PodbeanStream):
    name = 'podcast_analytic_report'
    path = '/v1/analytics/podcastAnalyticReports'
    schema_filepath = f'{SCHEMAS_DIR}/analytics_reports.json'

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[int]
        ) -> dict:
        return {'types[]': ['followers','likes','comments','total_episode_length']}

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        return {'podcast_id': 'network', **row}


class PodcastAnalyticReportsStream(_PodcastPartitionStream):
    name = 'podcast_analytic_report'
    path = '/v1/analytics/podcastAnalyticReports'
    schema_filepath = f'{SCHEMAS_DIR}/analytics_reports.json'

    @property
    def partitions(self) -> List[dict]:
        return [{'podcast_id':k} for k in self.authenticator.tokens.keys()]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[int]
    ) -> Dict[str, Any]:
        podcast_id = context.get('podcast_id')
        
        return {
            'access_token': self.authenticator.tokens.get(podcast_id),
            'podcast_id': podcast_id,
            'types[]': ['followers','likes','comments','total_episode_length']
        }

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        id = context.get('podcast_id')
        return {'podcast_id': id, **row}
