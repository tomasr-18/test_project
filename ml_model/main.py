from pipline_to_ml import get_data_by_company, calculate_rolling_average, transform_data_to_model,train_model#,scale_features
from fastapi import FastAPI#, HTTPException
from pydantic import BaseModel
from typing import Optional , List
import os
from dotenv import load_dotenv
#import logging
load_dotenv()
app = FastAPI()

# Definiera en modell för inkommande data


class ModelRequest(BaseModel):
    company_list: Optional[List[str]] = ['AAPL', 'GOOGL', 'TSLA', 'AMZN', 'MSFT']
    project_id: Optional[str] = 'tomastestproject-433206'
    dataset: Optional[str] = 'testdb_1'
    table_from: Optional[str] = 'avg_scores_and_stock_data_right'
    prediction_table: Optional[str] = 'model_predictions'
    #model_name = Optional[str] = "latest"


# Definiera POST endpoint för att hämta, rensa och analysera nyheter


@app.post("/train_model/")
def train_model_endpoint(request: ModelRequest):
    df=get_data_by_company(company=request.company_list)
    transformed_df=transform_data_to_model(df=df)
    transformed_rolling_avg = calculate_rolling_average(df=transformed_df,column_name="close",window_size=3)
    models = train_model(df=transformed_rolling_avg)
    return models
    
    


@app.post("/predict/")
def predict_endpoint(request: ModelRequest):
    asd = os.getenv("NEWS_API_KEY")
    company="AAPL"
    from_date="2024-09-01"
    to_date="2024-09-05"
    sort_by="relevance"
    language="en"
    url = f'https://newsapi.org/v2/everything?q={company}&from={from_date}&to={to_date}&sortBy={sort_by}&language={language}&apiKey={asd}'

    import requests
    response = requests.get(url=url)
    response.raise_for_status()  # Kontrollera för HTTP-fel
    data = response.json()

    # Kontrollera om API-anropet innehåller fel
    if data.get("status") != "ok":
        raise ValueError(
            f"API Error: {data.get('message', 'Unknown error')}")

    return data

@app.post("/transfer_targets/")
def transfer_target_endpoint(request: ModelRequest):
    pass
