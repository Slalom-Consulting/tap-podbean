"""Auth handling for PodbeanStream."""

from typing import Optional
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream
from singer_sdk.helpers._util import utc_now
import requests


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
    
    default_endpoint = '/v1/oauth/token'

    @property
    def auth_headers(self) -> dict:
        """[DO NOT REMOVE] Prevents auth_token from being passed to header from parent class""" 
        return {}

    @property
    def auth_endpoint(self) -> str:
        """Auth endpoint with basic auth included in path."""
        auth_endpoint = f'{self.url_base}{self.default_endpoint}'
        basic_auth = f'{self.client_id}:{self.client_secret}@'
        return f'{auth_endpoint}'.replace('://', f'://{basic_auth}')

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
    default_endpoint = '/v1/oauth/multiplePodcastsToken'
    _tokens = None

    @property
    def auth_params(self) -> dict:
        """[DO NOT REMOVE] Handled by partitions. Overrides parent class.""" 
        return {}

    @property
    def tokens(self) -> dict:
        """Gets tokens if not valid and stores auth tokens per podcast"""
        if not self.is_token_valid():
            self.update_access_token()

        return self._tokens

    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in` and `tokens`.
        Raises:
            RuntimeError: When OAuth login fails.
        """
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

        # Podcast auth
        self._tokens = {p['podcast_id']:p['access_token'] for p in token_json['podcasts']}
