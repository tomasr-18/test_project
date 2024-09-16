import os
import json
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from google.cloud import secretmanager
import uvicorn
import logging
from google.auth import default

app = FastAPI()

def get_project_id():
    """Retrieve project ID from environment."""
    #GOOGLE_CLOUD_PROJECT: This specific environment variable is used to store the Google Cloud project ID. 
    #It allows the application to know which Google Cloud project it should interact with.
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')

    if project_id is None:
        # If not set, use google.auth.default to fetch the project ID
        credentials, project_id = default()
    
    return project_id


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
    project_id = get_project_id()  # Ersätt med ditt projekt-ID
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Hämta den senaste versionen av hemligheten
    response = client.access_secret_version(name=secret_path)

    # Dekoda hemligheten till en sträng
    secret_data = response.payload.data.decode('UTF-8')

    return secret_data


# List of 5 specific stocks to focus on
STOCK_SYMBOLS = ["TSLA", "MSFT", "AMZN", "GOOGL", "AAPL"]

def get_latest_date_in_bigquery(client, table_id: str, stock_symbol: str):
    """
    Returns the latest date for the given stock_symbol in BigQuery.
    If no data exists, returns None.
    """
    query = f"""
        SELECT MAX(date) as latest_date
        FROM `{table_id}`
        WHERE stock_symbol = @stock_symbol
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("stock_symbol", "STRING", stock_symbol)
        ]
    )
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    for row in results:
        return row.latest_date
    return None

def clean_and_insert_latest_data(client, results, cleaned_data_table_id: str):
    """
    Cleans the latest day's stock data from BigQuery and inserts it into the cleaned_data_table in BigQuery.

    Args:
        client: BigQuery client instance.
        results: The query results from the raw data table.
        cleaned_data_table_id: The BigQuery table ID where the cleaned data will be inserted.
    """
    rows_to_insert = []
    
    for row in results:
        stock_symbol = row.stock_symbol
        raw_data_str = row.raw_data  # raw_data is expected to be in JSON format
        
        # Parse the JSON-like structure of raw_data_str
        time_series = raw_data_str.get("Time Series (Daily)", {})
        
        if time_series:
            latest_date = max(time_series.keys())
            latest_data = time_series[latest_date]
            
            cleaned_row = {
                "stock_symbol": stock_symbol,
                "date": latest_date,
                "open": float(latest_data["1. open"]),
                "high": float(latest_data["2. high"]),
                "low": float(latest_data["3. low"]),
                "close": float(latest_data["4. close"]),
                "volume": int(latest_data["5. volume"]),
            }
            rows_to_insert.append(cleaned_row)
    
    # Insert cleaned rows into BigQuery
    errors = client.insert_rows_json(cleaned_data_table_id, rows_to_insert)
    
    if errors:
        logging.error(f"Encountered errors while inserting rows: {errors}")
        raise HTTPException(status_code=500, detail=f"Error inserting rows: {errors}")
    
    logging.info("Cleaned data successfully inserted.")
    return {"status": "success", "message": "Latest data successfully inserted."}

def clean_and_insert_data(client, results, cleaned_data_table_id: str):
    """
    Cleans the raw stock data from BigQuery and inserts it into the cleaned_data_table in BigQuery.

    Args:
        client: BigQuery client instance.
        results: The query results from the raw data table.
        cleaned_data_table_id: The BigQuery table ID where the cleaned data will be inserted.
    """
    rows_to_insert = []
    
    for row in results:
        stock_symbol = row.stock_symbol
        raw_data_str = row.raw_data  # raw_data is expected to be in JSON format

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
    errors = client.insert_rows_json(cleaned_data_table_id, rows_to_insert)
    
    if errors:
        logging.error(f"Encountered errors while inserting rows: {errors}")
        raise HTTPException(status_code=500, detail=f"Error inserting rows: {errors}")
    
    logging.info("Cleaned data successfully inserted.")
    return {"status": "success", "message": "Cleaned data successfully inserted."}


def fetch_and_insert_historical_data(client, raw_data_table_id: str, cleaned_data_table_id: str, stock_symbol: str):
    """
    Fetches historical stock data for a specific stock symbol and inserts all records into BigQuery.
    """
    # Your existing function to fetch and insert all historical data
    logging.info(f"Fetching historical data for {stock_symbol}")
    
    # Fetch raw data
    query = f"""
        SELECT stock_symbol, raw_data
        FROM `{raw_data_table_id}`
        WHERE stock_symbol = @stock_symbol
    """
    query_job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("stock_symbol", "STRING", stock_symbol)]
    ))
    results = query_job.result()

    # Clean and insert historical data into BigQuery
    clean_and_insert_data(client, results, cleaned_data_table_id)

def fetch_and_insert_latest_data(client, raw_data_table_id: str, cleaned_data_table_id: str, stock_symbol: str):
    """
    Fetches the latest stock data for a specific stock symbol and inserts the newest record into BigQuery.
    """
    logging.info(f"Fetching latest data for {stock_symbol}")
    
    query = f"""
        SELECT stock_symbol, raw_data
        FROM `{raw_data_table_id}`
        WHERE stock_symbol = @stock_symbol
    """
    query_job = client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("stock_symbol", "STRING", stock_symbol)]
    ))
    results = query_job.result()

    # Clean and insert the latest data into BigQuery
    clean_and_insert_latest_data(client, results, cleaned_data_table_id)

@app.post("/clean-stock-data/")
def clean_stock_data():
    """
    Determines whether to fetch historical or daily stock data for TSLA, MSFT, AMZN, GOOGL, AAPL,
    and inserts it into BigQuery.
    """
    try:
        # Load secrets and set up BigQuery client
        secret_data = get_secret('bigquery-accout-secret')
        service_account_info = json.loads(secret_data)
        client = bigquery.Client.from_service_account_info(service_account_info)
        
        # Fetch table IDs from secrets
        raw_data_table_id = get_secret("RAW_DATA_TABLE_ID")
        cleaned_data_table_id = get_secret("CLEANED_DATA_TABLE_ID")

        for stock_symbol in STOCK_SYMBOLS:
            latest_date = get_latest_date_in_bigquery(client, cleaned_data_table_id, stock_symbol)

            if latest_date is None:
                # If no data exists, perform historical data fetch
                fetch_and_insert_historical_data(client, raw_data_table_id, cleaned_data_table_id, stock_symbol)
            else:
                # Fetch and insert only the latest data
                fetch_and_insert_latest_data(client, raw_data_table_id, cleaned_data_table_id, stock_symbol)

        return {"status": "success", "message": "Stock data updated successfully."}

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")