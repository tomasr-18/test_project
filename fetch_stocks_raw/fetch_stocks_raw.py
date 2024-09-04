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
from google.api_core.exceptions import GoogleAPIError, NotFound


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

# Load environment variables from .env file
load_dotenv(dotenv_path='/home/psor/testgit/test_project/get_stocks_raw/.env')

# Retrieve API key and project ID from environment variables
STOCK_API_KEY = os.getenv('STOCK_API_KEY')
PROJECT_ID = os.getenv('PROJECT_ID')

# Initialize BigQuery client with service account credentials
credentials = service_account.Credentials.from_service_account_file(
    "/mnt/c/Users/m_was/Downloads/tomastestproject-433206-48a55703dec2.json")
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Define the BigQuery table ID
table_id = f"{PROJECT_ID}.testdb_1.raw_stock_data"

# Initialize FastAPI app
app = FastAPI()

@app.post("/fetch-and-upload/")
async def fetch_and_upload_raw_stock_data(stock_symbol: str):
    """
    Fetches raw stock data from Alpha Vantage API and uploads it to BigQuery.
    """
    try:
        # Construct the API URL
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={STOCK_API_KEY}"
        response = requests.get(url=url)   
        response.raise_for_status()
        data = response.json()
        fetch_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Prepare data for insertion
        rows_to_insert = [
            {
                "stock_symbol": stock_symbol,
                "raw_data": json.dumps(data),
                "fetch_date": fetch_date  
            }
        ]
        
        # Insert rows into BigQuery
        errors = client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            raise HTTPException(status_code=500, detail=f"Encountered errors while inserting rows: {errors}")
        
        return JSONResponse(content={"message": "Rows successfully inserted."})
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

