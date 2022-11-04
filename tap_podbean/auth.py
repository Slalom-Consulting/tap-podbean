"""Auth handling for PodbeanStream."""

from typing import Optional
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream
from singer_sdk.helpers._util import utc_now
from urllib.parse import urljoin
from enum import Enum
import requests


class APIAuthType(str, Enum):
    default = '/v1/oauth/token'
    multi = '/v1/oauth/multiplePodcastsToken'


class PodbeanAuthenticator(OAuthAuthenticator):
    def __init__(
            self, stream: RESTStream,
            podcast_id: Optional[str] = None,
            default_expiration: Optional[int] = None
        ) -> None:
        """Create a new authenticator.
        Args:
            stream: The stream instance to use with this authenticator.
            podcast_id: [Optional] Return auth for a specific podcast. If None uses api default.
            default_expiration: [Optional] Default token expiry in seconds. If None uses api default.
        """
        expiration = default_expiration or stream.config.get('auth_expiration')
        super().__init__(stream=stream, default_expiration=expiration)
        self.url_base = stream.url_base
        self.podcast_id = podcast_id
    
    auth_type = APIAuthType['default'].value

    @property
    def auth_headers(self) -> dict:
        return {}  # Handled by auth_params. Overrides parent class

    @property
    def auth_endpoint(self) -> str:
        url = urljoin(self.url_base, self.auth_type)
        auth = f'{self.client_id}:{self.client_secret}@'
        return url.replace('://', f'://{auth}')

    @property
    def oauth_request_body(self) -> dict:
        payload = {'grant_type': 'client_credentials'}

        if self.podcast_id:
            payload['podcast_id'] = self.podcast_id

        return payload

    @property
    def auth_params(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        
        return {'access_token': self.access_token}


class PodbeanPartitionAuthenticator(PodbeanAuthenticator):
    """Authenticator with auth tokens for each podcast."""
    _tokens = None

    auth_type = APIAuthType['multi'].value

    @property
    def auth_params(self) -> dict:
        return {}  # Handled by Stream. Overrides parent class

    @property
    def tokens(self) -> dict:
        """Update and store a dict of auth tokens for each podcast."""
        if not self.is_token_valid():
            self.update_access_token()

        return self._tokens

    def update_access_token(self) -> None:
        # Cloned from parent class
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        token_response = requests.post(self.auth_endpoint, data=auth_request_payload)
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json.get("expires_in", self._default_expiration)
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time

        # Podcast auth for partitioning
        self._tokens = {p['podcast_id']:p['access_token'] for p in token_json['podcasts']}
