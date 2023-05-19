"""Stream type classes for tap-podbean."""

from pathlib import Path
from typing import Any, Dict, List, Optional

from tap_podbean.client import PodbeanCSVStream, PodbeanPartitionStream, PodbeanStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
ANALYTIC_REPORT_TYPES = ["followers", "likes", "comments", "total_episode_length"]


class PrivateMembersStream(PodbeanStream):
    name = "private_members"
    path = "/v1/privateMembers"
    records_jsonpath = "$.private_members[*]"
    primary_keys = ["email"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR.joinpath("private_members.json")


class PodcastsStream(PodbeanStream):
    name = "podcasts"
    path = "/v1/podcasts"
    records_jsonpath = "$.podcasts[*]"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR.joinpath("podcasts.json")


class EpisodesStream(PodbeanPartitionStream):
    name = "episodes"
    path = "/v1/episodes"
    records_jsonpath = "$.episodes[*]"
    primary_keys = ["id"]
    replication_key = None
    schema_filepath = SCHEMAS_DIR.joinpath("episodes.json")

    @property
    def partitions(self) -> List[dict]:
        return [{"podcast_id": k} for k in self.authenticator.tokens.keys()]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[dict]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        podcast_id = context.get("podcast_id") if context else None
        params["access_token"] = self.authenticator.tokens.get(podcast_id)
        return params


class PodcastDownloadReportsStream(PodbeanCSVStream):
    primary_keys = ["podcast_id", "file_key", "record_key"]
    name = "podcast_download_reports"
    path = "/v1/analytics/podcastReports"
    replication_key = None
    schema_filepath = SCHEMAS_DIR.joinpath("csv_reports.json")


class PodcastEngagementReportsStream(PodbeanCSVStream):
    primary_keys = ["podcast_id", "file_key", "record_key"]
    name = "podcast_engagement_reports"
    path = "/v1/analytics/podcastEngagementReports"
    replication_key = None
    schema_filepath = SCHEMAS_DIR.joinpath("csv_reports.json")


class PodcastAnalyticReportsStream(PodbeanPartitionStream):
    primary_keys = ["podcast_id"]
    name = "podcast_analytic_reports"
    path = "/v1/analytics/podcastAnalyticReports"
    replication_key = None
    schema_filepath = SCHEMAS_DIR.joinpath("analytics_reports.json")

    @property
    def partitions(self) -> List[dict]:
        return [{"podcast_id": k} for k in self.authenticator.tokens.keys()]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[dict]
    ) -> Dict[str, Any]:
        podcast_id = str(context.get("podcast_id")) if context else None
        return {
            "access_token": self.authenticator.tokens.get(podcast_id),
            "podcast_id": podcast_id,
            "types[]": ANALYTIC_REPORT_TYPES,
        }

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        # Add Podcast ID to record
        id = str(context.get("podcast_id")) if context else None
        return {"podcast_id": id, **row}
