import logging
from typing import List

from google.cloud import bigquery

from models.user_review_bigquery_dto import UserReviewBigQueryDto

BIGQUERY_TABLE = "book-suggestion-please.user_reviews.user_reviews_v1"
logger = logging.getLogger(__name__)


class BigQueryDao(object):
    def __init__(self):
        self.client = bigquery.Client()

    def write(self, rows_to_insert: List[UserReviewBigQueryDto]):
        errors = self.client.insert_rows_json(BIGQUERY_TABLE, [dto.dict() for dto in rows_to_insert])
        if not errors:
            logger.info(print(f"Successfully wrote {len(rows_to_insert)} rows!"))
        else:
            logger.warning("Encountered errors while inserting rows: {}".format(errors))
