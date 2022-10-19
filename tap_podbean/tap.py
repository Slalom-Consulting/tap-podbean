"""Podbean tap class."""

from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_podbean.streams import (
#    PodbeanStream,
    PrivateMembersStream,
    PodcastsStream,
    EpisodesStream,
    PodcastDownloadReportsStream,
    PodcastEngagementReportsStream,
    PodcastAnalyticReportsStream,
    NetworkAnalyticReportsStream,
)

STREAM_TYPES = [
    PrivateMembersStream,
    PodcastsStream,
    EpisodesStream,
    PodcastDownloadReportsStream,
    PodcastEngagementReportsStream,
    NetworkAnalyticReportsStream,
    PodcastAnalyticReportsStream, 
]

class TapPodbean(Tap):
    """Podbean tap class."""
    name = 'tap-podbean'

    config_jsonschema = th.PropertiesList(
        th.Property(
            'client_id',
            th.StringType,
            required=True,
            description='The token to authenticate against the API service'
        ),
        th.Property(
            'client_secret',
            th.StringType,
            required=True,
            description='Project IDs to replicate'
        ),
        th.Property(
            'start_date',
            th.DateTimeType,
            required=True,
            description='The earliest record date to sync'
        ),
        th.Property(
            'api_url',
            th.StringType,
            default='https://api.podbean.com',
            description='The url for the API service'
        ),
        th.Property(
            'auth_expires_in',
            th.IntegerType,
            description='[Optional] API default value: 604800; Size range: 60-604800'
        ),
        th.Property(
            'page_limit',
            th.IntegerType,
            description='[Optional] API default value: 20; Size range: 0-100'
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == '__main__':
    TapPodbean.cli()
