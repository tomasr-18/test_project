
from get_data import fetch_data_by_date
import pandas as pd
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from datetime import datetime, timedelta
from google.cloud import bigquery



def transform_data(df:pd.DataFrame,date:str)->pd.DataFrame:
    
    df['score_description'] = df['description'].apply(make_score)


    df['score_title'] = df['title'].apply(make_score)
    means = df.groupby(
        'company',)[['score_title', 'score_description']].mean()
    means.reset_index(inplace=True)
    means['pub_date'] = datetime.now().strftime(date)
    means['fetch_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return means

def write(data,  table='news_sentiment', project_id='tomastestproject-433206', dataset='testdb_1') -> None:

    # Initiera BigQuery-klienten
    client = bigquery.Client.from_service_account_json(
        'tomastestproject-433206-adc5bc090976.json')

    # Definiera din dataset och tabell
    table_id = f"{project_id}.{dataset}.{table}"
    # print(table_id)
    list_of_dicts = data.to_dict(orient='records')
    # print(rows_to_insert)
    client.get_table(table_id)
    errors = client.insert_rows_json(table_id, list_of_dicts)   
    print(errors)

def make_score(string):
    sia = SentimentIntensityAnalyzer()
    if string is None:
        return None
    else:
        return sia.polarity_scores(string)['compound']



def main(date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')):
    nltk.download([
        "vader_lexicon"
    ])
    data=fetch_data_by_date(date)
    data=transform_data(data,date)
    write(data)

if __name__=='__main__':
    main()
