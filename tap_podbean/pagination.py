"""Pagination handling for AdobeUmapiStream."""

from typing import Optional
from singer_sdk.pagination import JSONPathPaginator
from requests import Response

class PodbeanPaginator(JSONPathPaginator):
    def has_more(self, response: Response) -> bool:
        """Override this method to check if the endpoint has any pages left.
        Args:
            response: API response object.
        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return response.json().get('has_more', False) == True

    def get_next(self, response: Response) -> Optional[int]:
        """Get the next page token.
        Args:
            response: API response object.
        Returns:
            The next page token.
        """
        response_json = response.json()
        return response_json['offset'] + response_json['limit']
