"""Pagination handling for AdobeUmapiStream."""

from singer_sdk.pagination import BaseAPIPaginator
from requests import Response
from typing import Any, Union


class PodbeanPaginator(BaseAPIPaginator):
    """Paginator class for APIs that use page number."""

    def __init__(self) -> None:
        """Create a new paginator.
        """
        super().__init__(None)

    def get_next(self, response: Response) -> Union[int, None]:
        """Get the next page token.
        Args:
            response: API response object.
        Returns:
            The next page token.
        """
        data:dict = response.json()

        has_more:bool = data.get("has_more")
        
        if has_more:
            return data.get("offset") + 1
