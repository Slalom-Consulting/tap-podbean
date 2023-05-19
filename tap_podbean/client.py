"""REST client handling, including PodbeanStream base class."""

import csv
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlsplit

import requests
from memoization import cached
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_podbean.auth import PodbeanAuthenticator, PodbeanPartitionAuthenticator
from tap_podbean.pagination import PodbeanPaginator

PAGINATION_INDEX = 0
API_URL = "https://api.podbean.com"


class PodbeanStream(RESTStream):
    """Podbean stream class."""

    auth_type = "default"

    @property
    def url_base(self) -> str:
        return self.config.get("api_url", API_URL)

    @property
    def authenticator(self) -> PodbeanAuthenticator:
        @cached  # type: ignore[override]
        def _auth(self: RESTStream, auth_type: str = "default"):
            if auth_type == "multi":
                return PodbeanPartitionAuthenticator(self)
            return PodbeanAuthenticator(self)

        return _auth(self, self.auth_type)

    def get_new_paginator(self) -> PodbeanPaginator:
        limit = self.config.get("limit")
        page_size = int(limit) if limit else None
        return PodbeanPaginator(PAGINATION_INDEX, page_size)

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[dict]
    ) -> Dict[str, Any]:
        return {
            "offset": next_page_token or PAGINATION_INDEX,
            "limit": self.config.get("limit"),
        }


class PodbeanPartitionStream(PodbeanStream):
    """Base class for podcast partitions."""

    auth_type = "multi"


class PodbeanCSVStream(PodbeanPartitionStream):
    """Base class for CSV report streams."""

    records_jsonpath = "$.download_urls"
    _csv_requests_session = requests.Session()

    @property
    def csv_requests_session(self) -> requests.Session:
        return self._csv_requests_session

    def _csv_request(self, prepared_request) -> requests.Response:
        return self._csv_requests_session.send(
            prepared_request, stream=True, timeout=self.timeout
        )

    @staticmethod
    def _csv_timstamp(val: str) -> datetime:
        # Wed, 04 Jan 2023 04:49:49 GMT
        response_date_format = "%a, %d %b %Y %H:%M:%S %Z"
        return datetime.strptime(val, response_date_format)

    def _csv_response(
        self, url: str, json_path: Optional[str] = None
    ) -> requests.Response:
        request = requests.Request("GET", url=url)
        prepared_request = self.csv_requests_session.prepare_request(request)
        decorated_request = self.request_decorator(self._csv_request)
        response: requests.Response = decorated_request(prepared_request)

        parent_partition = json.loads(
            self.stream_state["partitions"][0]["context"]["partition"]
        )

        self._write_request_duration_log(
            endpoint=self.path,
            response=response,
            context={
                "podcast_id": parent_partition.get("podcast_id"),
                "year": parent_partition.get("year"),
                "csv_stream": True,
                "record_json_path": f"{self.records_jsonpath}.{json_path}",
            },
            extra_tags={"url": url} if self._LOG_REQUEST_METRIC_URLS else None,
        )

        return response

    def _csv_records(
        self, url: str, *args, **kwargs
    ) -> Iterable[Tuple[dict, int, requests.Response]]:
        """Read CSV using SDK Error Handeling."""
        response = self._csv_response(url, *args, **kwargs)
        decoded_file = (line.decode("utf-8-sig") for line in response.iter_lines())
        reader = csv.DictReader(decoded_file, delimiter=",")

        for record_num, record in enumerate(reader):
            yield record, record_num, response

    @property
    def start_date(self) -> datetime:
        return datetime.strptime(
            str(self.config.get("start_date")), "%Y-%m-%dT%H:%M:%S"
        )

    @property
    def partitions(self) -> List[dict]:
        def _get_years(start_year) -> List[int]:
            """List of years for CSV Reports."""
            current_year = datetime.utcnow().date().year

            if start_year < current_year:
                year_rng = range(current_year - start_year + 1)
                return [start_year + i for i in year_rng]

            elif start_year > current_year:
                return [start_year]

            return [current_year]

        def _json_str(podcast_id, year) -> str:
            """Parameters for CSV Reports."""
            part = {"podcast_id": podcast_id, "year": year}

            return json.dumps(part)

        # Work around for SDK limitation combining both a child context (id) and
        # partition (year)
        podcast_ids = [id for id in self.authenticator.tokens.keys()]
        years = _get_years(self.start_date.year)

        return [
            {"partition": _json_str(id, year)} for id in podcast_ids for year in years
        ]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[dict]
    ) -> Dict[str, Any]:
        parts: dict = json.loads(str(context.get("partition"))) if context else None
        podcast_id = parts.get("podcast_id")
        return {
            "access_token": self.authenticator.tokens.get(podcast_id),
            "podcast_id": podcast_id,
            "year": parts.get("year"),
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        records: dict = next(
            extract_jsonpath(self.records_jsonpath, input=response.json())
        )

        for month, vals in records.items():
            if not vals:
                continue

            vals = vals if isinstance(vals, list) else [vals]

            for i, url in enumerate(vals):
                if not urlsplit(str(url))[0] in ("https", "http"):
                    continue

                file_key = f"{month}_{i}"
                json_path = f"{month}_[{i}]"
                for record, record_key, csv_resp in self._csv_records(url, json_path):
                    last_modified = csv_resp.headers.get("Last-Modified")

                    yield {
                        "file_key": file_key,
                        "record_key": record_key,
                        "record_value": json.dumps(record),
                        "file_last_modified_at": last_modified,
                    }

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        # Add Podcast ID to record
        parts = json.loads(str(context.get("partition"))) if context else None
        id = str(parts.get("podcast_id"))
        return {"podcast_id": id, **row}
