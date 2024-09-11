from pipline_to_ml import get_data_by_company, calculate_rolling_average, transform_data_to_model, train_model, make_prediction, transform_predictions_for_bq, save_predictions_to_big_query, save_model, get_latest_date, insert_true_value_to_bigquery
from load_env import load_env_from_secret
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional , List
import os
from datetime import datetime, timedelta, timezone


load_env_from_secret(project_id=os.getenv("PROJECT_ID"),
                     secret_name=os.getenv("SECRET_NAME_ENV"))
app = FastAPI()

class ModelRequest(BaseModel):
    company_list: Optional[List[str]] = ['AAPL', 'GOOGL', 'TSLA', 'AMZN', 'MSFT']
    project_id: Optional[str] = os.getenv("PROJECT_ID")
    dataset: Optional[str] = os.getenv("DATA_SET")
    table_from: Optional[str] = 'avg_scores_and_stock_data_right'
    prediction_table: Optional[str] = 'predictions'


@app.post("/train_and_predict/")
def train_model_endpoint(request: ModelRequest):
    try:
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        latest_date = get_latest_date()

        # Check if there is data to predict from
        if latest_date != yesterday:
            return {"message": "No data to predict from."}

        # Fetch data by company
        try:
            df = get_data_by_company(
                                    company=request.company_list, 
                                    table=request.table_from,
                                    project_id=request.project_id,
                                    dataset=request.dataset)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error fetching data: {str(e)}")

        # Transform data to the model format
        try:
            transformed_df = transform_data_to_model(df)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error transforming data: {str(e)}")

        # Calculate rolling average
        try:
            df_rolling_avg = calculate_rolling_average(
                df=transformed_df, column_name="close", window_size=3)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error calculating rolling average: {str(e)}")

        # Train the model
        try:
            models, predictions_rows, date = train_model(df_rolling_avg)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error training the model: {str(e)}")

        # Make predictions
        try:
            y_preds = make_prediction(
                models_dict=models, x_pred=predictions_rows)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error making predictions: {str(e)}")

        # Transform predictions for BigQuery
        try:
            list_to_big_query, predicted_date = transform_predictions_for_bq(
                predictions=y_preds, date=date)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error transforming predictions for BigQuery: {str(e)}")

        # Save predictions to BigQuery
        try:
            save_predictions_to_big_query(
                                            data=list_to_big_query,
                                            table=request.prediction_table,
                                            project_id=request.project_id,
                                            dataset=request.dataset)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error saving predictions to BigQuery: {str(e)}")

        # Save the model
        try:
            save_model(model_dict=models,
                       date=predicted_date.strftime('%Y-%m-%d'))
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Error saving the model: {str(e)}")

        return {"status": "success", "message": "Model training and prediction completed successfully."}

    except Exception as e:
        # General catch-all error handling for unexpected issues
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {str(e)}")

@app.post("/get_true_values/")
def get_true_values_endpoint(request: ModelRequest):
   try:
    affected_rows=insert_true_value_to_bigquery(
                                    prediction_table=request.prediction_table, 
                                    table_from=request.table_from, 
                                    project_id=request.project_id,
                                    dataset=request.dataset
                                    )
    return {"messege": affected_rows}
   except Exception as e:
       raise HTTPException(
           status_code=500, detail=f"Error inserting true values to {request.prediction_table}: {str(e)}")
    
