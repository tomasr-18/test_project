from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
from google.cloud import secretmanager
import requests
import structlog
import json
import os
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Initialize logger
logger = structlog.get_logger()

# Initialize FastAPI app
app = FastAPI()

# Function to fetch secrets from Google Cloud Secret Manager
def get_secret(secret_name='bigquery-accout-secret') -> str:
    """Fetches a secret from Google Cloud Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        project_id = 'tomastestproject-433206' 
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        secret_data = response.payload.data.decode('UTF-8')
        return secret_data
    except Exception as e:
        logger.exception("Error fetching secret from Secret Manager", exc_info=True)
        raise 

# Load environment variables
STOCK_API_KEY = os.getenv('STOCK_API_KEY') or get_secret('stock-api-key')
PROJECT_ID = os.environ.get('PROJECT_ID') or get_secret('project-id')
RAW_DATA_TABLE_ID = os.getenv('RAW_DATA_TABLE_ID') or get_secret('raw-data-table-id')
CLEANED_DATA_TABLE_ID = os.getenv('CLEANED_DATA_TABLE_ID') or get_secret('clean-stock-data-table-id')

# Initialize BigQuery client
credentials = service_account.Credentials.from_service_account_info(json.loads(get_secret('bigquery-accout-secret')))
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Function to fetch stock data from Alpha Vantage API
def fetch_stock_data(stock_symbol: str) -> dict:
    try:
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={STOCK_API_KEY}"
        response = requests.get(url=url)
        response.raise_for_status()
        data = response.json()

        if "Error Message" in data:
            raise HTTPException(status_code=404, detail=f"Stock symbol '{stock_symbol}' not found.")

        return data

    except requests.RequestException as e:
        logger.error("Error fetching data from Alpha Vantage", exc_info=True)
        raise HTTPException(status_code=503, detail="Error fetching data from Alpha Vantage")

# Function to upload data to BigQuery
def upload_to_bigquery(stock_symbol: str, data: dict) -> None:
    try:
        fetch_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        rows_to_insert = [
            {
                "stock_symbol": stock_symbol,
                "raw_data": json.dumps(data),
                "fetch_date": fetch_date
            }
        ]

        errors = client.insert_rows_json(RAW_DATA_TABLE_ID, rows_to_insert)

        if errors:
            logger.error("Encountered errors while inserting rows", errors=errors)
            raise HTTPException(status_code=500, detail=f"Encountered errors while inserting rows: {errors}")

    except Exception as e:
        logger.exception("Error uploading data to BigQuery", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@app.post("/fetch-and-upload/")
async def fetch_and_upload_raw_stock_data(stock_symbol: str):
    """
    Fetches raw stock data from Alpha Vantage API and uploads it to BigQuery.
    """
    if not stock_symbol:
        raise HTTPException(status_code=400, detail="Stock symbol is required.")

    data = fetch_stock_data(stock_symbol)
    upload_to_bigquery(stock_symbol, data)

    return JSONResponse(content={"message": "Rows successfully inserted."})

# Run the FastAPI app
#if __name__ == "__main__":
#    import uvicorn
 #   uvicorn.run(app, host="0.0.0.0", port=8000)

