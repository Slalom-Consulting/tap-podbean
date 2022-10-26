"""REST client handling, including PodbeanStream base class."""

from typing import Any, Dict, Optional
from singer_sdk.streams import RESTStream
from tap_podbean.auth import PodbeanAuthenticator
from tap_podbean.pagination import PodbeanPaginator
from memoization import cached


class PodbeanStream(RESTStream):
    """Podbean stream class."""
    @property
    def url_base(self) -> str:
        """Return the base url for this API."""
        return self.config['api_url']

    @property
    @cached
    def authenticator(self) -> PodbeanAuthenticator:
        """Return a new authenticator object."""
        return PodbeanAuthenticator(self)

    def get_new_paginator(self) -> PodbeanPaginator:
        return PodbeanPaginator()

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[int]
        ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}

        if next_page_token:
            params['offset'] = next_page_token

            if 'page_limit' in self.config:
                params['limit'] = self.config['page_limit']
            
        return params
