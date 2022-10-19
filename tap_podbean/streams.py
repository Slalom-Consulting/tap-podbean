"""Stream type classes for tap-podbean."""

from typing import Any, Dict, Optional, List, Iterable
from pathlib import Path
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_podbean.auth import PodbeanPartitionAuthenticator
from tap_podbean.client import PodbeanStream
from datetime import datetime, date
import requests
import json
import csv
import re

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

class PodcastsStream(PodbeanStream):
    name = 'podcasts'
    path = '/v1/podcasts'
    records_jsonpath = '$.podcasts[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = get_schema_fp('podcasts')

class _PodcastPartitionStream(PodbeanStream):
    """Base class for podcast partitions"""
    @property
    def authenticator(self) -> PodbeanPartitionAuthenticator:
        return PodbeanPartitionAuthenticator(self)

class EpisodesStream(_PodcastPartitionStream):
    name = 'episodes'
    path = '/v1/episodes'
    records_jsonpath = '$.episodes[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = get_schema_fp('episodes')

    @property
    def partitions(self) -> List[dict]:
        return [{'podcast_id':k} for k in self.authenticator.tokens.keys()]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[int]
    ) -> Dict[str, Any]:
        podcast_id = context['podcast_id']
        auth = {'access_token': self.authenticator.tokens.get(podcast_id)}
        base_params = super().get_url_params(context, next_page_token)
        return {**auth, **base_params}

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

        if start_year < current_year:
            year_rng = range(current_year + 1 - start_year)
            years = [start_year + y for y in year_rng]
    
        elif start_year > current_year:
            years = [start_year]

        else:
            years = [current_year]
    
        def json_str(podcast_id, year):
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
        parts = json.loads(context['partition'])
        podcast_id = parts['podcast_id']
        auth = {'access_token': self.authenticator.tokens.get(podcast_id)}
        #base_params = super().get_url_params(None, next_page_token)
        params = {**auth}
        params['podcast_id'] = podcast_id
        params['year'] = parts['year']
        return params

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

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Add context to and filter row"""
        record_date_text = row.get(self.response_date_key).lstrip("'")
        record_date = datetime.strptime(record_date_text,'%Y-%m-%d %H:%M:%S')

        if record_date >= self.start_date:
            id = json.loads(context['partition'])['podcast_id']
            return {'podcast_id': id, **row}

class PodcastDownloadReportsStream(_CsvStream):
    name = 'podcast_download_reports'
    path = '/v1/analytics/podcastReports'
    schema_filepath = get_schema_fp('podcast_download_reports')
    response_date_key = 'Time(GMT)'

class PodcastEngagementReportsStream(_CsvStream):
    name = 'podcast_engagement_reports'
    path = '/v1/analytics/podcastEngagementReports'
    schema_filepath = get_schema_fp('podcast_engagement_reports')
    response_date_key = 'Time(GMT)'

class NetworkAnalyticReportsStream(PodbeanStream):
    name = 'podcast_analytic_report'
    path = '/v1/analytics/podcastAnalyticReports'
    schema_filepath = get_schema_fp('analytics_reports')

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[int]
        ) -> Dict[str, Any]:
        types = {'types[]': ['followers','likes','comments','total_episode_length']}
        return types

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        return {'podcast_id': 'network', **row}

class PodcastAnalyticReportsStream(_PodcastPartitionStream):
    name = 'podcast_analytic_report'
    path = '/v1/analytics/podcastAnalyticReports'
    schema_filepath = get_schema_fp('analytics_reports')

    @property
    def partitions(self) -> List[dict]:
        return [{'podcast_id':k} for k in self.authenticator.tokens.keys()]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[int]
    ) -> Dict[str, Any]:
        podcast_id = context['podcast_id']
        auth = {'access_token': self.authenticator.tokens.get(podcast_id)}
        types = {'types[]': ['followers','likes','comments','total_episode_length']}
        podcast = {'podcast_id': podcast_id}
        return {**auth, **types, **podcast}

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        id = context['podcast_id']
        return {'podcast_id': id, **row}
