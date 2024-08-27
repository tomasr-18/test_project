# from flask import Flask, request, jsonify
# from get_data import fetch_data_by_date
# import pandas as pd
# from nltk.sentiment import SentimentIntensityAnalyzer
# import nltk
# from datetime import datetime, timedelta
# from google.cloud import bigquery

# app = Flask(__name__)


# @app.route('/')
# def home():
#     return "<h1>Server is running!</h1><p>Welcome to the Flask application.</p>"


# @app.route('/process', methods=['POST'])
# def process_data():
#     # Get the date from the request JSON body, or default to yesterday's date
#     date = request.json.get(
#         'date', (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))

#     try:
#         # Run the main function with the provided date
#         result = main(date)

#         if result:
#             return jsonify({"message": "Data processed successfully!"}), 200
#         else:
#             return jsonify({"error": "No data found for the specified date."}), 404

#     except Exception as e:
#         return jsonify({"error": str(e)}), 500


# def main(date: str):
#     nltk.download("vader_lexicon")

#     # Fetch data based on the provided date
#     data = fetch_data_by_date(date)

#     if data.empty:
#         return None  # No data found

#     # Transform the data
#     transformed_data = transform_data(data, date)

#     # Write data to BigQuery
#     write(transformed_data)

#     return True


# def transform_data(df: pd.DataFrame, date: str) -> pd.DataFrame:
#     df['score_description'] = df['description'].apply(make_score)
#     df['score_title'] = df['title'].apply(make_score)

#     # Group by company and calculate mean scores
#     means = df.groupby('company')[
#         ['score_title', 'score_description']].mean().reset_index()
#     means['pub_date'] = date
#     means['fetch_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#     return means


# def write(data, table='news_sentiment', project_id='tomastestproject-433206', dataset='testdb_1') -> None:
#     client = bigquery.Client.from_service_account_json(
#         'tomastestproject-433206-adc5bc090976.json')
#     table_id = f"{project_id}.{dataset}.{table}"
#     list_of_dicts = data.to_dict(orient='records')
#     client.get_table(table_id)
#     errors = client.insert_rows_json(table_id, list_of_dicts)
#     if errors:
#         raise RuntimeError(f"Error inserting rows: {errors}")


# def make_score(string):
#     sia = SentimentIntensityAnalyzer()
#     if string is None:
#         return None
#     else:
#         return sia.polarity_scores(string)['compound']


# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=8000)

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from get_data import fetch_data_by_date
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from datetime import datetime, timedelta
from google.cloud import bigquery

# Initiera FastAPI-applikationen
app = FastAPI()

# Pydantic-modell för att validera inkommande JSON-data


class ProcessDataRequest(BaseModel):
    date: str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')


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


def main(date: str):
    nltk.download("vader_lexicon")

    # Hämta data baserat på angivet datum
    data = fetch_data_by_date(date)

    if data.empty:
        return None  # Ingen data hittades

    # Transformera data
    transformed_data = transform_data(data, date)

    # Skriv data till BigQuery
    write(transformed_data)

    return True


def transform_data(df: pd.DataFrame, date: str) -> pd.DataFrame:
    df['score_description'] = df['description'].apply(make_score)
    df['score_title'] = df['title'].apply(make_score)

    # Gruppera per företag och beräkna medelvärden
    means = df.groupby('company')[
        ['score_title', 'score_description']].mean().reset_index()
    means['pub_date'] = date
    means['fetch_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return means


def write(data, table='news_sentiment', project_id='tomastestproject-433206', dataset='testdb_1') -> None:
    client = bigquery.Client.from_service_account_json(
        'tomastestproject-433206-adc5bc090976.json')
    table_id = f"{project_id}.{dataset}.{table}"
    list_of_dicts = data.to_dict(orient='records')
    client.get_table(table_id)
    errors = client.insert_rows_json(table_id, list_of_dicts)
    if errors:
        raise RuntimeError(f"Error inserting rows: {errors}")


def make_score(string):
    sia = SentimentIntensityAnalyzer()
    if string is None:
        return None
    else:
        return sia.polarity_scores(string)['compound']
