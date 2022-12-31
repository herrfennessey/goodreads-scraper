from google.cloud import bigquery

class BigQueryDao(object):
    def __init__(self, logger):
        self.client = bigquery.Client()
        self.logger = logger

    def write(self, rows_to_insert, table_name):
        errors = self.client.insert_rows_json(table_name, [dto.dict() for dto in rows_to_insert])
        if not errors:
            self.logger.info(f"Successfully wrote {len(rows_to_insert)} rows to {table_name}!")
        else:
            self.logger.warning(f"Encountered errors while inserting rows: {errors}")
