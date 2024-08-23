import requests
from google.cloud import bigquery
from datetime import datetime,timedelta
from dotenv import load_dotenv
import os
import sys


def get_news_data(company: str, api_key: str, from_date=(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d'), to_date=datetime.now().strftime('%Y-%m-%d'), sort_by='relevance', language='en'):
    url = f'https://newsapi.org/v2/everything?q={company}&from={from_date}&to={to_date}&sortBy={sort_by}&language={language}&apiKey={api_key}'
    response = requests.get(url=url)
    
    print('from:',from_date)
    print('to: ',to_date)
    if response.status_code == 200:
        return response.json()
    else:
        print('Misslyckad request till newsapi', response.status_code)


def write(data, company: str, table='table_1', project_id='tomastestproject-433206', dataset='testdb_1') -> None:

    # Initiera BigQuery-klienten
    client = bigquery.Client.from_service_account_json(
        '/Users/tomasrydenstam/Downloads/tomastestproject-433206-adc5bc090976.json')

    # Definiera din dataset och tabell
    table_id = f"{project_id}.{dataset}.{table}"
    #print(table_id)
    
    rows_to_insert = []

    for row in data['articles']:
        row['source'] = row['source']['name']
        row['company'] = company
        row['fetch_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rows_to_insert.append(row)
    #print(rows_to_insert)

    errors = client.insert_rows_json(table_id, rows_to_insert)
    print(errors)


def main(company):
    load_dotenv()
    
    data = get_news_data(
        api_key=os.getenv('NEWS_API_KEY'), company=company)
    print(data)
    print(company)

    write(data=data,company=company)



if __name__=='__main__':
    company = sys.argv[1]
    
    main(company=company)