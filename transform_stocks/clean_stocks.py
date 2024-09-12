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

def fetch_raw_data(client, raw_data_table_id: str):
    """
    Fetch raw stock data from BigQuery.
    """
    query = f"""
        SELECT stock_symbol, raw_data
        FROM `{raw_data_table_id}`
    """
    query_job = client.query(query)
    results = query_job.result()
    return results

def clean_and_insert_data(client, results, cleaned_data_table_id: str):
    """
    Clean raw stock data and insert into BigQuery.
    """
    rows_to_insert = []
    for row in results:
        stock_symbol = row.stock_symbol
        raw_data_str = row.raw_data 
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

@app.post("/clean-stock-data/")
def clean_stock_data():
    """
    Fetches raw stock data from BigQuery, cleans it, and inserts the cleaned data back into BigQuery.
    """
    try:
        # Load secrets
        secret_data = get_secret('bigquery-accout-secret')
        service_account_info = json.loads(secret_data)
        client = bigquery.Client.from_service_account_info(service_account_info)
        
        # Fetch table IDs from secrets
        raw_data_table_id = get_secret("RAW_DATA_TABLE_ID")
        cleaned_data_table_id = get_secret("CLEANED_DATA_TABLE_ID")
        
        # Fetch raw data
        results = fetch_raw_data(client, raw_data_table_id)
        
        # Clean and insert data
        return clean_and_insert_data(client, results, cleaned_data_table_id)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)