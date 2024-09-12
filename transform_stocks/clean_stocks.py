import os
import json
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from google.cloud import secretmanager
import uvicorn
import logging

app = FastAPI()

def get_secret(secret_name: str) -> str:
    """Fetches a secret from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    project_id = 'tomastestproject-433206'
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_path)
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
        
        # Parse raw_data_str from string to JSON
        raw_data_json = json.loads(raw_data_str)
        time_series = raw_data_json.get("Time Series (Daily)", {})
        
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
        
        # Fetch other secrets
     
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