"""Pagination handling for AdobeUmapiStream."""

from typing import Optional, Any
from singer_sdk.pagination import BaseAPIPaginator
from requests import Response

class PodbeanPaginator(BaseAPIPaginator):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Create a new paginator.
        Args:
            args: Paginator positional arguments for base class.
            kwargs: Paginator keyword arguments for base class.
        """
        super().__init__(None, *args, **kwargs)

    def has_more(self, response: Response) -> bool:
        """Check if the endpoint has any pages left.
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
        page_keys = ['offset', 'limit']
        page = {k:v for k,v in response.json().items() if k in page_keys}
        return page['offset'] + page['limit']
