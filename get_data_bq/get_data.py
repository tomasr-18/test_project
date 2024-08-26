

from google.cloud import bigquery
from google.cloud import secretmanager

import os
import pandas as pd
import db_dtypes
from dotenv import load_dotenv


def fetch_data_by_date(pub_date=None, table='table_1', project_id='tomastestproject-433206', dataset='testdb_1'):
    #load_dotenv()
    # Ange sökvägen till din service account JSON-fil
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'get_data_bq/tomastestproject-433206-adc5bc090976.json'

    table_id = f"{project_id}.{dataset}.{table}"
    # Skapa en BigQuery-klient
    client = bigquery.Client()

 
    query = f"""
        SELECT company,title,description,publishedAt
        FROM `{table_id}`
        WHERE LEFT(publishedAt, 10) = '{pub_date}';

        """

    query_job = client.query(query)

    # Hämta resultaten
    results = query_job.result()

    return results.to_dataframe()


#Exempel på användning av funktionen
if __name__ == "__main__":
    fetch_data_by_date("2024-08-20")
    # secret = access_secret_version(
    #     name='projects/839243415895/secrets/test-github-oauthtoken-e3deeb')
    # print(f"Fetched secret: {secret}")
