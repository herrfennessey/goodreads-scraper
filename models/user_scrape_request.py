from typing import List

from pydantic import BaseModel


class UserScrapeRequest(BaseModel):
    profiles: List[str]
    persist: bool = True
