from pipline_to_ml import get_data_by_company, calculate_rolling_average, transform_data_to_model,train_model#,scale_features
from fastapi import FastAPI#, HTTPException
from pydantic import BaseModel
from typing import Optional , List
#import logging

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
    pass


@app.post("/transfer_targets/")
def transfer_target_endpoint(request: ModelRequest):
    pass
