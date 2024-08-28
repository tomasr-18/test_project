import os
import db_dtypes
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from datetime import datetime, timedelta



def fetch_news_data_by_date_from_bq(pub_date:str, table='table_1', project_id='tomastestproject-433206', dataset='testdb_1')-> pd.DataFrame:
    # Set the path to your service account JSON file

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'tomastestproject-433206-adc5bc090976.json'

    table_id = f"{project_id}.{dataset}.{table}"
    # Create a BigQuery client
    client = bigquery.Client()

    # Build your SQL query
    query = f"""
        SELECT company, title, description, publishedAt
        FROM `{table_id}`
        WHERE LEFT(publishedAt, 10) = '{pub_date}'
    """

    # Execute the SQL query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a DataFrame
    df = results.to_dataframe()

    # Check if DataFrame is empty and raise an error if needed
    if df.empty:
        raise ValueError(f"No data found for the given date: {pub_date}")

    return df


def make_score(string:str)-> float:
    """
    Predicts sentiment for a string. returns a float between -1 and 1.
    """
    nltk.download("vader_lexicon")
    sia = SentimentIntensityAnalyzer()
    if string is None:
        return None
    else:
        return sia.polarity_scores(string)['compound']
    
    
def transform_data(df: pd.DataFrame, date: str) -> pd.DataFrame:
    """
    Makes scores for each title and description and aggregates the score for each company for each pub date.
    Also adds date of modification as "fetch_date".
    """
    df['score_description'] = df['description'].apply(make_score)
    df['score_title'] = df['title'].apply(make_score)

    # Gruppera per företag och beräkna medelvärden
    means = df.groupby('company')[
        ['score_title', 'score_description']].mean().reset_index()
    means['pub_date'] = date
    means['fetch_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return means


def write_to_big_query(data:pd.DataFrame, table='news_sentiment', project_id='tomastestproject-433206', dataset='testdb_1'):
    """
    Inserts values for: company, score_title, score_description, pub_date and fetch_date to Big Query.
    """
    client = bigquery.Client.from_service_account_json(
        'tomastestproject-433206-adc5bc090976.json')
    table_id = f"{project_id}.{dataset}.{table}"
    list_of_dicts = data.to_dict(orient='records')
    client.get_table(table_id)
    errors = client.insert_rows_json(table_id, list_of_dicts)
    if errors:
        raise RuntimeError(f"Error inserting rows: {errors}")


#Exempel på användning av funktionen
if __name__ == "__main__":
    #fetch_data_by_date("2024-08-20")
    pass
