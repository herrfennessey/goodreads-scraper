from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field

BOOKS_TABLE = "book-suggestion-please.books.books-v1"


class BooksBigQueryDto(BaseModel):
    book_title: str = Field(alias="title")
    book_url: str = Field(alias="url")
    avg_rating: float
    genres: List[str]
    isbn: Optional[str]
    isbn13: Optional[str]
    asin: Optional[str]
    language: Optional[str]
    num_pages: Optional[int]
    num_ratings: int
    publish_date: Optional[str]
    rating_histogram: str = Field()
    series: Optional[str]
    author: Optional[str]
    author_url: Optional[str]
    ingest_time: str = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    class Config:
        allow_population_by_field_name = True
