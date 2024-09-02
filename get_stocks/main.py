from get_stocks import fetch_and_upload_raw_stock_data
from get_stocks import clean_stock_data
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os

app = FastAPI()

class StockRequest(BaseModel):
    company: str
    api_key: str

@app.post("/get_stocks/")
def fetch_and_save_stocks(params: StockRequest):
    try:
        fetch_and_upload_raw_stock_data(
            stock_symbol=params.company,
            API_KEY=params.api_key
        )
        clean_stock_data()
        return {"message": "Data fetched and saved successfully."}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)