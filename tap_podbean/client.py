"""REST client handling, including PodbeanStream base class."""

from typing import Any, Dict, Optional
from pathlib import Path
from singer_sdk.streams import RESTStream
from tap_podbean.auth import PodbeanAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
import requests


class PodbeanStream(RESTStream):
    """Podbean stream class."""
    @property
    def url_base(self) -> str:
        """Return the base url for this API."""
        return self.config['api_url']

    @property
    def authenticator(self) -> PodbeanAuthenticator:
        """Return a new authenticator object."""
        return PodbeanAuthenticator(self)

    next_page_token_jsonpath = '$.has_more'

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        if self.next_page_token_jsonpath:
            data = response.json()
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, data
            )
            first_match = next(iter(all_matches), None)

            if first_match:
                return data['offset'] + data['limit']

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[int]
        ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        
        if next_page_token:
            params['offset'] = next_page_token

            page_limit:int = self.config.get('page_limit')
            if page_limit:
                params['limit'] = page_limit  
            
        return params
