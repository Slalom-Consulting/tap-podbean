"""Pagination handling for AdobeUmapiStream."""

from typing import Any, Optional

from requests import Response
from singer_sdk.pagination import BaseOffsetPaginator

PAGE_SIZE = 20
PAGE_SIZE_MAX = 100


class PodbeanPaginator(BaseOffsetPaginator):
    def __init__(
        self,
        start_value: int,
        page_size: Optional[int] = None,
        *args: Any,
        **kwargs: Any
    ) -> None:
        page_size = min(page_size or PAGE_SIZE, PAGE_SIZE_MAX)
        super().__init__(start_value, page_size, *args, **kwargs)

    def has_more(self, response: Response) -> bool:
        has_more: bool = response.json().get("has_more", False)
        return has_more
