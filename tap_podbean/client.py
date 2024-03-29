"""REST client handling, including PodbeanStream base class."""

from typing import Any, Dict, Optional

# from memoization import cached
from singer_sdk.streams import RESTStream

from tap_podbean.auth import PodbeanAuthenticator
from tap_podbean.pagination import PodbeanPaginator

PAGINATION_INDEX = 0
API_URL = "https://api.podbean.com"


class PodbeanStream(RESTStream):
    """Podbean stream class."""

    @property
    def url_base(self) -> str:
        return self.config.get("api_url", API_URL)

    @property
    # @cached
    def authenticator(self) -> PodbeanAuthenticator:
        return PodbeanAuthenticator(self)

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
