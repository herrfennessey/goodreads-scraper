from datetime import datetime
from typing import List, Optional, Dict

from pydantic import BaseModel, Field, validator

BOOKS_TABLE = "book-suggestion-please.book.books-v1"

class GenreList(BaseModel):
    list: List[Dict[str, str]]

class BooksBigQueryDto(BaseModel):
    book_title: str = Field(alias="title")
    book_url: str = Field(alias="url")
    avg_rating: float
    genres: GenreList
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

    @validator("genres", pre=True, always=True)
    def validate_date(cls, input):
        list_of_dicts = []
        for element in input:
            list_of_dicts.append({"element": element})
        return GenreList(list=list_of_dicts)

    class Config:
        allow_population_by_field_name = True
