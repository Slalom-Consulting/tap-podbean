"""Mock API."""
# import datetime
import json

import requests_mock

# from singer_sdk.testing import get_standard_tap_tests

# from tap_podbean.tap import TapPodbean

API_URL = "https://api.podbean.com"

# SAMPLE_CONFIG = {
#     "client_id": "SampleClientId",
#     "client_secret": "SampleClientSecret",
#     "start_date": datetime.datetime.now(datetime.timezone.utc).strftime(
#         "%Y-%m-%dT%H:%M:%S"
#     ),
# }
#
# test = [
#     test
#     for test in get_standard_tap_tests(TapPodbean, config=SAMPLE_CONFIG)
#     if test.__name__ in ("_test_stream_connections")
# ][0]

mock_responses_path = "tap_podbean/tests/mock_responses"

mock_responses = {
    "default_auth": {
        "type": "auth",
        "endpoint": "/v1/oauth/token",
        "file": "auth_default.json",
    },
    "multi_auth": {
        "type": "auth",
        "endpoint": "/v1/oauth/multiplePodcastsToken",
        "file": "auth_multi.json",
    },
    "episodes": {
        "type": "stream",
        "endpoint": "/v1/episodes",
        "file": "episodes.json",
    },
    "podcast_download_reports": {
        "type": "stream",
        "endpoint": "/v1/analytics/podcastReports",
        "file": "podcast_download_reports.json",
    },
    "podcast_engagement_reports": {
        "type": "stream",
        "endpoint": "/v1/analytics/podcastEngagementReports",
        "file": "podcast_engagement_reports.json",
    },
    "analytic_reports": {
        "type": "stream",
        "endpoint": "/v1/analytics/podcastAnalyticReports",
        "file": "analytic_reports.json",
    },
    "podcasts": {
        "type": "stream",
        "endpoint": "/v1/podcasts",
        "file": "podcasts.json",
    },
    "private_members": {
        "type": "stream",
        "endpoint": "/v1/privateMembers",
        "file": "private_members.json",
    },
    "csv_report": {
        "type": "csv_stream",
        "url": "https://mock.mock/SampleReport.csv",
        "file": "report.csv",
    },
}


def mock_api(func, SAMPLE_CONFIG):
    """Mock API."""

    def wrapper():
        with requests_mock.Mocker() as m:
            for k, v in mock_responses.items():
                path = f"{mock_responses_path}/{v['file']}"

                if v["type"] == "auth":
                    auth = "{}:{}@".format(
                        SAMPLE_CONFIG["client_id"], SAMPLE_CONFIG["client_secret"]
                    )

                    url = f"{API_URL}{v['endpoint']}"
                    url = url.replace("://", f"://{auth}")

                    with open(path, "r") as f:
                        response = json.load(f)

                    m.post(url, json=response)

                elif v["type"] == "stream":
                    url = f"{API_URL}{v['endpoint']}"

                    with open(path, "r") as f:
                        response = json.load(f)

                    m.get(url, json=response)

                elif v["type"] == "csv_stream":
                    url = v["url"]

                    with open(path, "rb") as f:
                        response = f.read()

                    m.get(url, content=response)

            func()

    wrapper()
