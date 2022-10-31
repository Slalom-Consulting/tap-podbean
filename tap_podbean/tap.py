"""Podbean tap class."""

from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

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
            required = True,
            description = 'The client identifier to authenticate against the API service'
        ),
        th.Property(
            'client_secret',
            th.StringType,
            required = True,
            description = 'The client secret to authenticate against the API service'
        ),
        th.Property(
            'start_date',
            th.DateTimeType,
            required = True,
            description = 'The earliest datetime (UTC) to sync records'
        ),
        th.Property(
            'api_url',
            th.StringType,
            default='https://api.podbean.com',
            description = 'The url for the API service'
        ),
        th.Property(
            'auth_expiration',
            th.IntegerType,
            description = 'Expiraton in seconds for auth. (Default: 604800; Range: 60-604800)'
        ),
        th.Property(
            'limit',
            th.IntegerType,
            default=100,
            description = 'The number of records to return per page. (Default: 100; Range: 0-100)'
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == '__main__':
    TapPodbean.cli()
