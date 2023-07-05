from google.oauth2 import service_account
from google.cloud import bigquery
import json


def load_to_bigquery(source_file_path: str, table_name: str):
    """
    Loads the processed data to BigQuery.

    Args:
        source_file_path: A string representing the path to the file to be loaded.
        table_name: A string representing the name of the table to load the data to.
    """

    # Set the path to your service account key file
    key_path = "dags/service_account_key.json"

    # Set the credentials using the service account key
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    # Instantiate the BigQuery client with the credentials
    client = bigquery.Client(credentials=credentials)

    # Refer to the table where the data will be loaded
    dataset_ref = client.dataset("youtube")
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)

    # Load the data from the json file to BigQuery
    with open(source_file_path, "r") as f:
        json_data = json.load(f)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job = client.load_table_from_json(json_data, table, job_config=job_config)
    job.result()  # Waits for the job to complete

    # Log the job results
    print(f"Loaded {job.output_rows} rows to {table.table_id}")


load_to_bigquery("tmp_file_processed.json", "trending_videos_test")
