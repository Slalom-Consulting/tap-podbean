"""Stream type classes for tap-podbean."""
from __future__ import annotations
from typing import Any, Dict, Optional, List, Iterable
from tap_podbean.auth import PodbeanPartitionAuthenticator
from tap_podbean.client import PodbeanStream
from memoization import cached
from pathlib import Path
from urllib.parse import urlsplit
from datetime import datetime, date
from singer_sdk.helpers.jsonpath import extract_jsonpath
import csv
import json
import requests
import re

SCHEMAS_DIR = Path(__file__).parent / Path('./schemas')
ANALYTIC_REPORT_TYPES = ['followers', 'likes', 'comments', 'total_episode_length']


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


class _BasePodcastPartitionStream(PodbeanStream):
    """Base class for podcast partitions"""
    @property
    @cached
    def authenticator(self) -> PodbeanPartitionAuthenticator:
        return PodbeanPartitionAuthenticator(self)


class EpisodesStream(_BasePodcastPartitionStream):
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


class _BaseCSVStream(_BasePodcastPartitionStream):
    """Base class for CSV report streams"""
    response_date_key = None # configure per stream
    records_jsonpath = '$.download_urls'

    _csv_requests_session = None

    @property
    def csv_requests_session(self) -> requests.Session:
        if not self._csv_requests_session:
            self._csv_requests_session = requests.Session()
        return self._csv_requests_session

    def _csv_request(self, prepared_request) -> requests.Response:
        return self._csv_requests_session.send(prepared_request, stream=True, timeout=self.timeout)

    @staticmethod
    def _csv_file_name(url: str) -> str:
        return urlsplit(url).path.split('/')[-1]

    @staticmethod
    def _csv_timstamp(val: str) -> datetime:
        response_date_format = '%a, %d %b %Y %H:%M:%S %Z'  # Wed, 04 Jan 2023 04:49:49 GMT
        return datetime.strptime(val, response_date_format)

    def _csv_response(self, url: str) -> requests.Response:
        request = requests.Request('GET', url=url)
        prepared_request = self.csv_requests_session.prepare_request(request)
        decorated_request = self.request_decorator(self._csv_request)
        response:requests.Response = decorated_request(prepared_request)
        file_last_modified_at = self._csv_timstamp(response.headers.get('Last-Modified'))
        
        if file_last_modified_at < self.start_date:
            return

        self._write_request_duration_log(
            endpoint=self.path,
            response=response,
            context={'file_name': self._csv_file_name(url)},
            extra_tags={"url": url}
            if self._LOG_REQUEST_METRIC_URLS
            else None,
        )

        return response

    def _csv_read_lines(self, url: str) -> Iterable(dict):
        """Read CSV using SDK Error Handeling"""
        response = self._csv_response(url)
        
        attributes = {
            '_file_name': self._csv_file_name(url),
            '_file_last_modified_at': str(self._csv_timstamp(response.headers.get('Last-Modified'))),
        }

        decoded_file = (line.decode('utf-8-sig') for line in response.iter_lines())
        reader = csv.DictReader(decoded_file, delimiter=',')

        for i, row in enumerate(reader):
            # Add metadata to record
            yield {**row, **attributes, '_file_row_num': i}

#    def _is_valid_key(self, val) -> bool:
#        """Only get valid keys and reduce excess CSV downloads"""
#        url_key_pattern = re.compile(r'^\d{4}-\d{1,2}$')
#        
#        if url_key_pattern.match(val):
#            report_month = datetime.strptime(val,'%Y-%m').date()
#            start_month = date(self.start_date.year, self.start_date.month, 1)
#            return report_month >= start_month
#
#    @staticmethod
#    def _extract_url(val) -> str:
#        """Flatten list if presented"""
#        return val[0] if isinstance(val, list) else val


    @property
    def start_date(self) -> datetime:
        return datetime.strptime(self.config.get('start_date'), '%Y-%m-%dT%H:%M:%S')

    @property
    def partitions(self) -> List[dict]:
        def _get_years(start_year) -> List[int]:
            """List of years for CSV Reports"""
            current_year = datetime.utcnow().date().year

            if start_year < current_year:
                year_rng = range(current_year - start_year + 1)
                return [start_year + i for i in year_rng]
        
            elif start_year > current_year:
                return [start_year]

            return [current_year]
    
        def _json_str(podcast_id, year) -> str:
            """Parameters for CSV Reports"""
            part = {
                'podcast_id': podcast_id,
                'year': year
            }
            
            return json.dumps(part)

        # Work around for SDK limitation combining both a child context (id) and partition (year)
        podcast_ids = [id for id in self.authenticator.tokens.keys()]
        years = _get_years(self.start_date.year)
        return [{'partition':_json_str(id, year)} for id in podcast_ids for year in years]

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[int]
        ) -> Dict[str, Any]:
        parts: dict = json.loads(context.get('partition'))
        podcast_id = parts.get('podcast_id')
        return {
            'access_token': self.authenticator.tokens.get(podcast_id),
            'podcast_id': podcast_id,
            'year': parts.get('year')
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        records = next(extract_jsonpath(self.records_jsonpath, input=response.json()))
        urls_list = ([v] if not isinstance(v, list) else v for v in records.values() if v)
        urls = (url for l in urls_list for url in l if urlsplit(url)[0])

        for url in urls:
            for row in self._csv_read_lines(url):
                yield row

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        record_date_text = row.get(self.response_date_key)

#        if record_date_text:
#            # Clean date key text
#            record_date_text = str(record_date_text).lstrip("'")
#            row[self.response_date_key] = record_date_text
#            
#            # Filter records
#            record_date = datetime.strptime(record_date_text,'%Y-%m-%d %H:%M:%S')
#
#            if record_date < self.start_date:
#                return None

        # Add Podcast ID to record
        parts: dict = json.loads(context.get('partition'))
        id: str = parts.get('podcast_id')
        return {'podcast_id': id, **row}


class PodcastDownloadReportsStream(_BaseCSVStream):
    primary_keys = ['podcast_id', '_file_name', '_file_row_num']
    name = 'podcast_download_reports'
    path = '/v1/analytics/podcastReports'
    replication_key = None
    schema_filepath = f'{SCHEMAS_DIR}/podcast_download_reports.json'
    #response_date_key = 'Time(GMT)'


class PodcastEngagementReportsStream(_BaseCSVStream):
    primary_keys = ['podcast_id', '_file_name', '_file_row_num']
    name = 'podcast_engagement_reports'
    path = '/v1/analytics/podcastEngagementReports'
    replication_key = None
    schema_filepath = f'{SCHEMAS_DIR}/podcast_engagement_reports.json'
    #response_date_key = 'Time(GMT)'


class NetworkAnalyticReportsStream(PodbeanStream):
    primary_keys = ['podcast_id']
    name = 'network_analytic_reports'
    path = '/v1/analytics/podcastAnalyticReports'
    replication_key = None
    schema_filepath = f'{SCHEMAS_DIR}/analytics_reports.json'

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[int]
        ) -> dict:
        return {'types[]': ANALYTIC_REPORT_TYPES}

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        # Add Podcast ID to record
        return {'podcast_id': 'network', **row}


class PodcastAnalyticReportsStream(_BasePodcastPartitionStream):
    primary_keys = ['podcast_id']
    name = 'podcast_analytic_reports'
    path = '/v1/analytics/podcastAnalyticReports'
    replication_key = None
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
            'types[]': ANALYTIC_REPORT_TYPES
        }

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        # Add Podcast ID to record
        id = context.get('podcast_id')
        return {'podcast_id': id, **row}
