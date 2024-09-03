import os
import sys
import time
import json
from datetime import datetime
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from google.cloud import secretmanager
import structlog

# Initialize structured logging
logger = structlog.get_logger()

def get_secret(secret_name='bigquery-accout-secret') -> str:
    """Fetches a secret from Google Cloud Secret Manager.

    Args:
        secret_name (str): The name of the secret in Secret Manager.

    Returns:
        str: The secret data as a string.
    """
    # Instansiera en klient för Secret Manager
    client = secretmanager.SecretManagerServiceClient()

    # Bygg sökvägen till den hemlighet du vill hämta
    project_id = 'tomastestproject-433206'  # Ersätt med ditt projekt-ID
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Hämta den senaste versionen av hemligheten
    response = client.access_secret_version(name=secret_path)

    # Dekoda hemligheten till en sträng
    secret_data = response.payload.data.decode('UTF-8')

    return secret_data

# Load environment variables
STOCK_API_KEY = os.getenv('STOCK_API_KEY') or get_secret('stock-api-key')
PROJECT_ID = os.environ.get('PROJECT_ID') or get_secret('project-id')
RAW_DATA_TABLE_ID = os.getenv('RAW_DATA_TABLE_ID') or get_secret('raw-data-table-id')
CLEANED_DATA_TABLE_ID = os.getenv('CLEANED_DATA_TABLE_ID') or get_secret('clean-stock-data-table-id')

# Initialize BigQuery client
# Initialize FastAPI app
app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Welcome to the Stock Data API"}

@app.post("/clean-stock-data/")
def clean_stock_data():
    """
    Fetches raw stock data from BigQuery, cleans it, and inserts the cleaned data back into BigQuery.
    """
    secret_data = get_secret()

    service_account_info = json.loads(secret_data)

    # Initiera BigQuery-klienten med service account
    client = bigquery.Client.from_service_account_info(service_account_info)

    try:
        ## Query to fetch raw data from BigQuery
        query = f"""
            SELECT stock_symbol, raw_data
            FROM `{RAW_DATA_TABLE_ID}`
        """
        query_job = client.query(query)
        results = query_job.result()

        rows_to_insert = []
        for row in results:
            stock_symbol = row.stock_symbol
            raw_data_str = json.loads(row.raw_data)  # Parse the JSON string
            
            # Parse the JSON-like structure of raw_data_str
            time_series = raw_data_str.get("Time Series (Daily)", {})
            for date, daily_data in time_series.items():
                cleaned_row = {
                    "stock_symbol": stock_symbol,
                    "date": date,
                    "open": float(daily_data["1. open"]),
                    "high": float(daily_data["2. high"]),
                    "low": float(daily_data["3. low"]),
                    "close": float(daily_data["4. close"]),
                    "volume": int(daily_data["5. volume"]),
                }
                rows_to_insert.append(cleaned_row)

        # Insert cleaned rows into BigQuery
        errors = client.insert_rows_json(CLEANED_DATA_TABLE_ID, rows_to_insert)
        if errors:
            print(f"Encountered errors while inserting rows: {errors}")
            raise HTTPException(status_code=500, detail=f"Error inserting rows: {errors}")
        else:
            print("Cleaned data successfully inserted.")
            return {"status": "success", "message": "Cleaned data successfully inserted."}

    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

