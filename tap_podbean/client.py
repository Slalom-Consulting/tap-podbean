"""REST client handling, including PodbeanStream base class."""

from typing import Any, Dict, Optional
from singer_sdk.streams import RESTStream
from tap_podbean.auth import PodbeanAuthenticator
from tap_podbean.pagination import PodbeanPaginator
from memoization import cached

PAGINATION_INDEX = 0
API_URL = 'https://api.podbean.com'


class PodbeanStream(RESTStream):
    """Podbean stream class."""
    @property
    def url_base(self) -> str:
        """Return the base url for this API."""
        return self.config.get('api_url', API_URL)

    @property
    @cached
    def authenticator(self) -> PodbeanAuthenticator:
        """Return a new authenticator object."""
        return PodbeanAuthenticator(self)

    def get_new_paginator(self) -> PodbeanPaginator:
        page_size = self.config.get('limit')
        return PodbeanPaginator(PAGINATION_INDEX, page_size)

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[dict]
        ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        return {
            'offset': next_page_token or PAGINATION_INDEX,
            'limit': self.config.get('limit')
        }
