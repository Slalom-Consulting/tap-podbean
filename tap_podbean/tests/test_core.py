"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_podbean.tap import TapPodbean
from tap_podbean.tests.mock_api import mock_api

SAMPLE_CONFIG = {
    "client_id": "SampleClientId",
    "client_secret": "SampleClientSecret",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    ),
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapPodbean, config=SAMPLE_CONFIG)
    for test in tests:
        if test.__name__ in ("_test_stream_connections"):
            mock_api(test, SAMPLE_CONFIG)
            continue

        test()


# TODO: Create additional tests as appropriate for your tap.
