import logging

from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQueryDao(object):
    def __init__(self):
        self.client = bigquery.Client()

    def write(self, rows_to_insert, table_name):
        errors = self.client.insert_rows_json(table_name, [dto.dict() for dto in rows_to_insert])
        if not errors:
            logger.info(print(f"Successfully wrote {len(rows_to_insert)} rows to {table_name}!"))
        else:
            logger.warning(f"Encountered errors while inserting rows: {errors}")
