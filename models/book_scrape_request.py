from typing import List

from pydantic import BaseModel


class BookScrapeRequest(BaseModel):
    book_urls: List[str]
    persist: bool = True
