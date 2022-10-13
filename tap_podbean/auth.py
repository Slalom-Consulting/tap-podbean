"""Auth handling for PodbeanStream."""

from singer_sdk.authenticators import OAuthAuthenticator

class PodbeanAuthenticator(OAuthAuthenticator):

    @property
    def auth_endpoint(self) -> str:
        basic_auth = f'{self.client_id}:{self.client_secret}@'
        return f'{self._auth_endpoint}'.replace('://', f'://{basic_auth}')

    @property
    def oauth_request_body(self) -> dict:
        payload = {'grant_type': 'client_credentials'}

        # Optional parameter, uses default podcast otherwise
        # podcast_id:str = self.podcast_id
        # if podcast_id:
        #     payload['podcast_id'] = podcast_id

        return payload

    @property
    def auth_headers(self) -> dict:
        """do not remove; overrides default auth_headers"""
        return {}

    @property
    def auth_params(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        
        return {'access_token': self.access_token}
