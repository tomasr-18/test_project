
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from sentiment import fetch_news_data_by_date_from_bq, transform_data,write_to_big_query
from google.cloud import bigquery

# Initiera FastAPI-applikationen
app = FastAPI()

# Pydantic-modell för att validera inkommande JSON-data

class ProcessDataRequest(BaseModel):
    date: str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


def main(date: str):
    pass

@app.get("/")
def home():
    return {
        "message": "Server is running!",
        "description": "Welcome to the FastAPI application."
    }


@app.post("/process")
def process_data(request: ProcessDataRequest):
    try:
        # Kör huvudfunktionen med det datum som skickas in
        result = main(request.date)

        if result:
            return {"message": "Data processed successfully!"}
        else:
            raise HTTPException(
                status_code=404, detail="No data found for the specified date.")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))





