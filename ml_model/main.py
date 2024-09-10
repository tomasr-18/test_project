from pipline_to_ml import get_data_by_company, calculate_rolling_average, transform_data_to_model, train_model, make_prediction, transform_predictions_for_bq, save_predictions_to_big_query, save_model, get_latest_date, insert_true_value_to_bigquery
from load_env import load_env_from_secret
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional , List
import os
from datetime import datetime, timedelta, timezone
#import logging

load_env_from_secret(project_id=os.getenv("PROJECT_ID"),
                     secret_name=os.getenv("SECRET_NAME_ENV"))
app = FastAPI()


# Definiera en modell för inkommande data


class ModelRequest(BaseModel):
    company_list: Optional[List[str]] = ['AAPL', 'GOOGL', 'TSLA', 'AMZN', 'MSFT']
    project_id: Optional[str] = os.getenv("PROJECT_ID")
    dataset: Optional[str] = os.getenv("DATA_SET")
    table_from: Optional[str] = 'avg_scores_and_stock_data_right'
    prediction_table: Optional[str] = 'predictions'

# Definiera POST endpoint för att hämta, rensa och analysera nyheter

@app.post("/train_and_predict/")
def train_model_endpoint(request: ModelRequest):
    yesterday  = (datetime.now(timezone.utc) - timedelta(days=1)
                            ).date()
    if get_latest_date() != yesterday:
        return {"messege":"no data to predict from"}
    else:
        try:
            df = get_data_by_company(company=request.company_list,
                                    table=request.table_from)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error fetching data: {str(e)}")

        try:
            transformed_df = transform_data_to_model(df)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error transforming data: {str(e)}")

        try:
            df_rolling_avg = calculate_rolling_average(
                df=transformed_df, column_name="close", window_size=3)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error calculating rolling average: {str(e)}")

        try:
            models, predictions_rows, date = train_model(df_rolling_avg)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error training model: {str(e)}")

        try:
            y_preds = make_prediction(models_dict=models, x_pred=predictions_rows)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error making predictions: {str(e)}")

        try:
            list_to_big_query = transform_predictions_for_bq(
                predictions=y_preds, date=date)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error transforming predictions for BigQuery: {str(e)}")

        try:
            save_predictions_to_big_query(data=list_to_big_query,table="predictions_copy")
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error saving predictions to BigQuery: {str(e)}")

        try:
            save_model(model_dict=models, date=date.strftime('%Y-%m-%d'))
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error saving model: {str(e)}")

        return {"status": "success", "message": "Model training and prediction completed successfully."}
    
    

@app.post("/get_true_values/")
def get_true_values_endpoint(request: ModelRequest):
   insert_true_value_to_bigquery()
    

@app.post("/transfer_targets/")
def transfer_target_endpoint(request: ModelRequest):
    pass
