import requests
from google.cloud import bigquery
#from dotenv import load_env

def write(table_id:str):

    # Initiera BigQuery-klienten
    client = bigquery.Client()

    # Definiera din dataset och tabell
    table_id = "your-project.your_dataset.your_table"

    # Data du vill skicka (kan vara en lista med dictionaries)
    rows_to_insert = [
        {"column1": "value1", "column2": "value2"},
        {"column1": "value3", "column2": "value4"},
    ]

    # Använd insert_rows_json för att skicka data till tabellen
    errors = client.insert_rows_json(table_id, rows_to_insert)

    # Hantera eventuella fel
    if errors == []:
        print("Data skickades till BigQuery!")
    else:
        print("Följande fel uppstod:", errors)

def get_data(api_key:str,date=None):
    url=f'http://api.weatherapi.com/v1/current.json?key={api_key}&q=bulk'
    response=requests.get(url=url)
    if response.status_code==200:
        return response.json()
    else:
        print('fel!')


def main():
    data=get_data('9a0a847704c34ec6b84124735242208')
    print(data)
    

if __name__=='__main__':
    main()