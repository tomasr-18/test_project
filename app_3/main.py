import requests
from google.cloud import bigquery
#from dotenv import load_env

def write(data,table='test_table',project_id='tomastestproject-433206',dataset='testdb_1')->None:

    # Initiera BigQuery-klienten
    client = bigquery.Client.from_service_account_json(
        '/Users/tomasrydenstam/Downloads/tomastestproject-433206-adc5bc090976.json')

    # Definiera din dataset och tabell
    table_id = f"{project_id}.{dataset}.{table}"

    # # Data du vill skicka (kan vara en lista med dictionaries)
    # rows_to_insert = [
    #     {"column1": "value1", "column2": "value2"},
    #     {"column1": "value3", "column2": "value4"},
    # ]

    # Använd insert_rows_json för att skicka data till tabellen
    errors = client.insert_rows_json(table_id, data)

    # Hantera eventuella fel
    if errors == []:
        print("Data skickades till BigQuery!")
    else:
        print("Följande fel uppstod:", errors)


def get_data(api_key:str,date=None):
    #url=f'http://api.weatherapi.com/v1/current.json?key={api_key}&q=bulk'
    url=f'http://api.weatherapi.com/v1/forecast.json?key={api_key}&q=London&days=10&aqi=no&alerts=no'
    response=requests.get(url=url)
    if response.status_code==200:
        return response.json()
    else:
        print('fel!')


def main():
    data=get_data('9a0a847704c34ec6b84124735242208')
    print(data)
    write(data=data)



if __name__=='__main__':
    main()