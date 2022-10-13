"""REST client handling, including PodbeanStream base class."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from isort import stream
from memoization import cached
from singer_sdk.streams import RESTStream
from tap_podbean.auth import PodbeanAuthenticator


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
        endpoint = f'{self.url_base}/v1/oauth/token'
        expiration:int = self.config.get('auth_expires_in')

        return PodbeanAuthenticator(self,
            auth_endpoint=endpoint,
            default_expiration=expiration
        )

    @property
    def page_limit(self) -> Union[int, None]:
        """Return the page limit for paginated streams."""
        return None


    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}

        if self.page_limit:
            params['limit'] = self.page_limit

        if next_page_token:
            params["offset"] = next_page_token

        if context:
            stream_params:dict = context
            if stream_params:
                for k, v in stream_params.items():
                    params[k] = v

        return params
