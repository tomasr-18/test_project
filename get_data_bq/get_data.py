

from google.cloud import bigquery
from google.cloud import secretmanager

import os
import pandas as pd
import db_dtypes
from dotenv import load_dotenv


from google.cloud import bigquery
import os
import pandas as pd


def fetch_data_by_date(pub_date=None, table='table_1', project_id='tomastestproject-433206', dataset='testdb_1'):
    # Set the path to your service account JSON file
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'get_data_bq/tomastestproject-433206-adc5bc090976.json'

    table_id = f"{project_id}.{dataset}.{table}"
    # Create a BigQuery client
    client = bigquery.Client()

    # Build your SQL query
    query = f"""
        SELECT company, title, description, publishedAt
        FROM `{table_id}`
        WHERE LEFT(publishedAt, 10) = '{pub_date}'
    """

    # Execute the SQL query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a DataFrame
    df = results.to_dataframe()

    # Check if DataFrame is empty and raise an error if needed
    if df.empty:
        raise ValueError(f"No data found for the given date: {pub_date}")

    return df





#Exempel på användning av funktionen
if __name__ == "__main__":
    fetch_data_by_date("2024-08-20")
    # secret = access_secret_version(
    #     name='projects/839243415895/secrets/test-github-oauthtoken-e3deeb')
    # print(f"Fetched secret: {secret}")
