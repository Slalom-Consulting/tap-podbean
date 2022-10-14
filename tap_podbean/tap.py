"""Podbean tap class."""

import re
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_podbean.streams import (
    PrivateMembersStream,
    PodbeanStream,
    PodcastsStream,
    EpisodesStream,
    PodcastDownloadReportsStream,
    PodcastEngagementReportsStream,
#    GroupsStream,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
#    PrivateMembersStream,
    PodcastsStream,
#    EpisodesStream,
    PodcastDownloadReportsStream,
#    PodcastEngagementReportsStream,
#    GroupsStream,
]


class TapPodbean(Tap):
    """Podbean tap class."""
    name = 'tap-podbean'

    # TODO: Update this section with the actual config values you expect:
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
            description='[Optional] Default value: 604800; Size range: 60-604800'
        ),
        th.Property(
            'page_limit',
            th.IntegerType,
            description='[Optional] Default value: 20; Size range: 0-100'
        ),
        #th.Property(
        #    'process_download_urls',
        #    th.BooleanType,
        #    default=False,
        #    description='[Optional] Default value: false. When set to true, streams returning download urls will stream the contents of the file.'
        #),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == '__main__':
    TapPodbean.cli()
