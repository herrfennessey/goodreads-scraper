from typing import List

from pydantic import BaseModel


class UserScrapeRequest(BaseModel):
    profiles: List[str]
    debug: bool = False
