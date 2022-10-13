"""Auth handling for PodbeanStream."""

from typing import Union
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream

class PodbeanAuthenticator(OAuthAuthenticator):
    def __init__(
        self,
        stream: RESTStream,
        podcast_id: Union[str, None] = None,
        default_expiration: Union[int, None] = None,
    ) -> None:
        """Create a new authenticator.
        Args:
            stream: The stream instance to use with this authenticator.
            podcast_id: API username.
            default_expiration: Default token expiry in seconds.
        """
        expiration = default_expiration or stream.config.get('auth_expires_in') or None
        super().__init__(stream=stream, default_expiration=expiration)
        self.url_base = stream.url_base
        self.podcast_id = podcast_id

    # override default population of auth_headers
    @property
    def auth_headers(self) -> dict:
        """do not remove""" 
        return {}

    @property
    def auth_endpoint(self) -> str:
        auth_endpoint = f'{self.url_base}/v1/oauth/token'
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
