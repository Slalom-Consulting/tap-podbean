"""Pagination handling for AdobeUmapiStream."""

from typing import Any
from singer_sdk.pagination import BaseOffsetPaginator
from requests import Response

PAGINATION_DEFAULT = 20
PAGINATION_MAX = 100


class PodbeanPaginator(BaseOffsetPaginator):
    def __init__(
        self,
        start_value: int,
        page_size: int = PAGINATION_DEFAULT,
        *args: Any,
        **kwargs: Any
    ) -> None:
        if page_size > PAGINATION_MAX:
            page_size = PAGINATION_MAX

        super().__init__(start_value, page_size, *args, **kwargs)

    def has_more(self, response: Response) -> bool:
        """Check if the endpoint has any pages left.
        Args:
            response: API response object.
        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return response.json().get('has_more', False) == True
