from fastapi import FastAPI
from fastapi.responses import JSONResponse
from google.cloud import bigquery
import os
from dotenv import load_dotenv
import requests

load_dotenv()
API_KEY = os.getenv('API_KEY')
PROJECT_ID = os.environ.get('PROJECT_ID')  

# No need to explicitly provide credentials when running on Cloud Run ,The BigQuery client will automatically use the service account associated with the Cloud Run service
client = bigquery.Client(project=PROJECT_ID)

table_id = f"{PROJECT_ID}.testdb_1.table_6"

app = FastAPI()

@app.get("/fetch_and_upload/{stock_symbol}")
def fetch_and_upload_stock_data(stock_symbol: str):
    try:
        # Construct the API URL
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={API_KEY}"
        response = requests.get(url=url)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()

        if data and "Time Series (Daily)" in data:
            time_series = data["Time Series (Daily)"]
            rows_to_insert = [
                {
                    "date": date,
                    "open": float(daily_data["1. open"]),
                    "high": float(daily_data["2. high"]),
                    "low": float(daily_data["3. low"]),
                    "close": float(daily_data["4. close"]),
                    "volume": int(daily_data["5. volume"]),
                    "stock_symbol": stock_symbol
                }
                for date, daily_data in time_series.items()
            ]

            # Insert rows into BigQuery
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if not errors:
                return JSONResponse({"message": "New rows have been added."})
            else:
                return JSONResponse({"error": f"Encountered errors while inserting rows: {errors}"}, status_code=500)
        else:
            return JSONResponse({"error": "Invalid or empty response from Alpha Vantage API"}, status_code=500)

    except requests.exceptions.RequestException as e:
        return JSONResponse({"error": f"An error occurred during the API request: {e}"}, status_code=500)