"""Stream type classes for tap-podbean."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from requests import request
from singer_sdk import typing as th  # JSON schema typing helpers
import json
import requests

from tap_podbean.client import PodbeanStream
from tap_podbean.paginator import PodbeanPaginator


SCHEMAS_DIR = Path(__file__).parent / Path('./schemas')

def get_schema_fp(file_name) -> str:
    return f'{SCHEMAS_DIR}/{file_name}.json'


class _PodbeanPaginatedStream(PodbeanStream):
    def get_new_paginator(self) -> PodbeanPaginator:
        """Get a fresh paginator for this API endpoint."""
        return PodbeanPaginator()

    @property
    def page_limit(self):
        return self.config.get('page_limit')


class PrivateMembersStream(_PodbeanPaginatedStream):
    """Define custom stream."""
    name = 'private_members'
    path = '/v1/privateMembers'
    records_jsonpath = '$.private_members[*]'
    primary_keys = ['email']
    replication_key = None
    schema_filepath = get_schema_fp('private_members')


class PodcastsStream(PodbeanStream):
    """Define custom stream."""
    name = 'podcasts'
    path = '/v1/podcasts'
    records_jsonpath = '$.podcasts[*]'
    primary_keys = ['id']
    replication_key = None
    schema_filepath = get_schema_fp('podcasts')

    def get_child_context(self, record: dict, context: dict | None) -> dict:
        #TODO: pass along year info here
        return {
            'podcast_id': record['id'],
            'year': 2021
        }


class _PodbeanReportDownloadStream(PodbeanStream):
    parent_stream_type = PodcastsStream

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        return {k:v for k, v in row.items() if v is not False}


class PodcastEngagementReportsStream(_PodbeanReportDownloadStream):
    """Define custom stream."""
    name = 'podcast_engagement_reports'
    path = '/v1/analytics/podcastEngagementReports'
    records_jsonpath = '$.download_urls'
    primary_keys = ['id']
    replication_key = None
    state_partitioning_keys = ["podcast_id", "year"]
    year = 2021


    @property
    def schema(self) -> dict:
        """Schema with dynamic key names."""
        fp = get_schema_fp('podcast_engagement_reports')
        
        with open(fp, 'r') as f:
            schema = json.load(f)

        record_names = [f'{self.year}-{m:02}' for m in range(1,13)]
        record_schema = schema.pop('patternProperties').get('^\\d{4}-\\d{1,2}$')
        schema['properties'] = {record:record_schema for record in record_names}

        return schema

    #def post_process(self, row: dict, context: dict | None = None) -> dict | None:
    #    #print('-'*10)
#
    #    out = {self.stream_params.get('podcast_id'):{}}
#
    #    for record_name in row.keys():
    #        record = row.get(record_name)
    #        urls = [url for url in record if url is not None]
    #        
    #        #for url in urls:
    #        #    response = requests.get(url)
    #        #    text = response.iter_lines()
    #        #    reader = csv.reader(text, delimiter=',')
    #        out['record_name'] = urls
#
    #    
#
    #    #print('-'*10)
    #    return out



#class GroupsStream(PodbeanStream):
#    """Define custom stream."""
#    name = "engagement reports"
#    path = "/analytics/podcastEngagementReports"
#    records_jsonpath= "$.private_members"
#    primary_keys = ["id"]
#    replication_key = "modified"
#    schema = th.PropertiesList(
#        th.Property("name", th.StringType),
#        th.Property("id", th.StringType),
#        th.Property("modified", th.DateTimeType),
#    ).to_dict()
