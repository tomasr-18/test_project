import os
import json
from datetime import datetime, timezone
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import secretmanager
import uvicorn
from pydantic import BaseModel
from google.auth import default

def get_project_id():
    """Retrieve project ID from environment."""
    #GOOGLE_CLOUD_PROJECT: This specific environment variable is used to store the Google Cloud project ID. 
    #It allows the application to know which Google Cloud project it should interact with.
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')

    if project_id is None:
        # If not set, use google.auth.default to fetch the project ID
        credentials, project_id = default()
    
    return project_id

STOCK_API_KEY = None  # Initialize as None

def get_stock_api_key():
    global STOCK_API_KEY
    if STOCK_API_KEY is None:  # Only fetch the key if it's not already set
        STOCK_API_KEY = get_secret('STOCK_API_KEY')
    return STOCK_API_KEY


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
 
PROJECT_ID = get_project_id()
RAW_DATA_TABLE_ID = get_secret('RAW_DATA_TABLE_ID') 


app = FastAPI()


# Define a Pydantic model for the stock request
class StockRequest(BaseModel):
    stock_symbol: str

def fetch_raw_stock_data(stock_symbol: str) -> dict:
    """
    Fetches raw stock data from Alpha Vantage API and returns the data.

    Args:
        stock_symbol: The stock symbol to fetch data for.

    Returns:
        dict: The raw stock data fetched from the API.
    """
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={STOCK_API_KEY}'
    
    try:
        response = requests.get(url=url)
        response.raise_for_status()
        data = response.json()

        if "Information" in data and "Alpha Vantage" in data["Information"]:
            raise ValueError(f"API Rate Limit Exceeded: {data['Information']}")


        if "Error Message" in data:
            raise ValueError(f"API Error: {data['Error Message']}")

        return data

    except requests.exceptions.RequestException as e:
        print(f"Network error or connection problem: {e}")
        raise HTTPException(status_code=500, detail=f"Network error: {e}")

    except ValueError as ve:
        print(f"Error in API response: {ve}")
        raise HTTPException(status_code=500, detail=f"API response error: {ve}")

def save_raw_stock_data(stock_symbol: str, stock_data : dict, raw_data_table_id: str) -> JSONResponse:
    """
    Saves raw stock data to BigQuery.

    Args:
        stock_symbol (str): The stock symbol.
        stock_data (dict): The raw stock data to save.
        raw_data_table_id (str): The ID of the BigQuery table to save the data to.
    """
    try: 
        fetch_date = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        # Create a BigQuery client
        credentials = service_account.Credentials.from_service_account_info(json.loads(get_secret()))
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

        # Prepare data for insertion
        rows_to_insert = [
            {
                "stock_symbol": stock_symbol,
                "raw_data": json.dumps(stock_data),
                "fetch_date": fetch_date  
            }
        ]

        # Insert rows into BigQuery
        errors = client.insert_rows_json(raw_data_table_id, rows_to_insert)

        if errors:
            raise HTTPException(status_code=500, detail=f"Encountered errors while inserting rows: {errors}")

        return JSONResponse(content={"message": "Rows successfully inserted."})

    except Exception as e:
        print(f"An error occurred while saving data: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred while saving data: {e}")

@app.post("/raw-stock-data/")
async def handle_raw_stock_data(stock_request: StockRequest):
    try:
        # Fetch raw stock data
        data = fetch_raw_stock_data(stock_request.stock_symbol)
    

        # Save raw stock data to BigQuery
        save_raw_stock_data(stock_symbol=stock_request.stock_symbol, stock_data=data, raw_data_table_id=get_secret('RAW_DATA_TABLE_ID') ) 
        
        return JSONResponse(content={"message": "Rows successfully inserted."})
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
