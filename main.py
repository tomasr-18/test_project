from flask import Flask, request, jsonify
import requests
from google.cloud import bigquery
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

app = Flask(__name__)

# Ladda miljövariabler från .env-filen
load_dotenv()

# Definiera funktion för att hämta nyhetsdata


def get_news_data(company: str, api_key: str, from_date=(datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d'), to_date=datetime.now().strftime('%Y-%m-%d'), sort_by='relevance', language='en'):
    url = f'https://newsapi.org/v2/everything?q={company}&from={from_date}&to={to_date}&sortBy={sort_by}&language={language}&apiKey={api_key}'
    response = requests.get(url=url)

    # print('from:', from_date)
    # print('to: ', to_date)
    if response.status_code == 200:
        return response.json()
    else:
        print('Misslyckad request till newsapi', response.status_code)
        return None

# Definiera funktion för att skriva data till BigQuery


def write(data, company: str, table='table_1', project_id='tomastestproject-433206', dataset='testdb_1') -> None:
    client = bigquery.Client.from_service_account_json(
        '/Users/tomasrydenstam/Downloads/tomastestproject-433206-adc5bc090976.json')

    table_id = f"{project_id}.{dataset}.{table}"
    rows_to_insert = []

    for row in data['articles']:
        row['source'] = row['source']['name']
        row['company'] = company
        row['fetch_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        rows_to_insert.append(row)

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(errors)
    else:
        print("Inga fel vid insättning av rader.")

# Definiera API-endpoint för att hämta nyheter och skriva till BigQuery


@app.route('/fetch_news', methods=['POST'])
def fetch_news():
    company = request.json.get('company')

    if not company:
        return jsonify({"error": "Företagsnamn saknas"}), 400

    data = get_news_data(api_key=os.getenv('NEWS_API_KEY'), company=company)

    if data:
        write(data=data, company=company)
        return jsonify({"message": "Data inskriven i BigQuery"}), 200
    else:
        return jsonify({"error": "Misslyckades att hämta nyhetsdata"}), 500


if __name__ == '__main__':
   
    app.run(host='0.0.0.0', port=8080)
    