"""REST client handling, including PodbeanStream base class."""

from typing import Any, Dict, Optional
from pathlib import Path
from singer_sdk.streams import RESTStream
from tap_podbean.auth import PodbeanAuthenticator
from tap_podbean.paginator import PodbeanPaginator

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class PodbeanStream(RESTStream):
    """Podbean stream class."""

    @property
    def url_base(self) -> str:
        """Return the base url for this API."""
        return self.config.get("api_url")

    @property
    def authenticator(self) -> PodbeanAuthenticator:
        """Return a new authenticator object."""
        return PodbeanAuthenticator(self)

    def get_new_paginator(self) -> PodbeanPaginator:
        """Get a fresh paginator for this API endpoint."""
        return PodbeanPaginator()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[int]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        
        if next_page_token:
            params["offset"] = next_page_token

            page_limit:int = self.config.get('page_limit')
            if page_limit:
                params['limit'] = page_limit  
            
        return params
