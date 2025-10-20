# storage/bigquery_loader.py
"""
Uploads validated weather data to Google BigQuery
"""
from google.cloud import bigquery
from loguru import logger
import pandas as pd

class BigQueryLoader:
    def __init__(self, project_id: str, dataset_id: str, table_id: str):
        self.client = bigquery.Client(project=project_id)
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"

    def upload_records(self, records):
        """
        Uploads a list of weather records (dicts) to BigQuery
        """
        if not records:
            logger.warning("No records to upload to BigQuery.")
            return 0

        df = pd.DataFrame(records)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"  # Adds new rows each time
        )

        logger.info(f"Uploading {len(df)} rows to {self.table_ref}")
        job = self.client.load_table_from_dataframe(df, self.table_ref, job_config=job_config)
        job.result()  # Wait for job to complete

        logger.info(f"✅ Uploaded {len(df)} rows to BigQuery table {self.table_ref}")
        return len(df)
