from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class UserReviewBigQueryDto(BaseModel):
    user_id: int
    user_id_slug: str
    author_link: str
    author_name: str
    book_link: str
    book_name: str
    date_read: Optional[str]
    date_added: Optional[str]
    user_rating: int
    ingest_time: str = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
