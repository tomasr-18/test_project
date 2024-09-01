import pandas as pd
from google.cloud import bigquery
import os
from nltk.sentiment import SentimentIntensityAnalyzer
from google.cloud import secretmanager
import json

def get_secret(secret_name='bigquery-accout-secret') -> str:
    """Fetches a secret from Google Cloud Secret Manager.

    Args:
        secret_name (str): The name of the secret in Secret Manager.

    Returns:
        str: The secret data as a string.
    """
    # Instansiera en klient för Secret Manager
    client = secretmanager.SecretManagerServiceClient()

    # Bygg sökvägen till den hemlighet du vill hämta
    project_id = 'tomastestproject-433206'  # Ersätt med ditt projekt-ID
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Hämta den senaste versionen av hemligheten
    response = client.access_secret_version(name=secret_path)

    # Dekoda hemligheten till en sträng
    secret_data = response.payload.data.decode('UTF-8')

    return secret_data

def get_raw_news_from_big_query(table='raw_news', project_id='tomastestproject-433206', dataset='testdb_1') -> pd.DataFrame:
    """ 
   Fetches raw news data from a specified BigQuery table and returns it as a pandas DataFrame.

    This function connects to Google BigQuery using a service account, executes an SQL query to select all rows
    from the specified table within the given dataset and project, and returns the results as a pandas DataFrame.
    """

    secret_data = get_secret()

    # Ladda JSON-strängen till en dictionary
    service_account_info = json.loads(secret_data)

    # Initiera BigQuery-klienten med service account
    client = bigquery.Client.from_service_account_info(
        service_account_info)

    table_id = f"{project_id}.{dataset}.{table}"
    # Create a BigQuery client
    

    # Build your SQL query
    query = f"""
        SELECT *
        FROM `{table_id}`
    """

    # Execute the SQL query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a DataFrame
    df = results.to_dataframe()

    # Check if DataFrame is empty and raise an error if needed
    if df.empty:
        raise ValueError(f"No data found")

    return df


def clean_news(df: pd.DataFrame) -> pd.DataFrame:
    """
        Cleans and transforms raw news data extracted from BigQuery into a structured DataFrame format.

    This function takes a DataFrame containing raw news data, unpacks JSON-like structures to separate rows 
    for each news article, and normalizes the data into a flat table format. The resulting DataFrame will have 
    one row per article with relevant information such as author, description, publication date, title, URL, 
    source, company, and sentiment scores.

    """
    # Förbered DataFrame
    # Se till att 'data' kolumnen är en lista av artiklar
    df['data'] = df['data'].apply(lambda x: x.get(
        'articles', []) if isinstance(x, dict) else [])

    # Explodera artiklar till separata rader
    df_exploded = df.explode('data')

    # Normalisera JSON-data i 'data' kolumnen
    articles_df = pd.json_normalize(df_exploded['data'])

    # Lägg till övriga kolumner
    # Kombinera normaliserad artikeldata med 'company' kolumnen
    final_df = pd.concat(
        [articles_df, df_exploded[['company']].reset_index(drop=True)], axis=1)

    final_df.drop(columns=['content', 'source.id', 'urlToImage'], inplace=True)

    final_df['publishedAt'] = pd.to_datetime(
        final_df['publishedAt'], format='%Y-%m-%dT%H:%M:%SZ', utc=True)

    final_df.rename(columns={"source.name": "source_name",
                             "publishedAt": "pub_date"},
                    inplace=True
                    )
    return final_df


def make_sentiment_score(string: str) -> float:
    """
    Predicts sentiment for a string. returns a float between -1 and 1.
    """
    sia = SentimentIntensityAnalyzer()
    if string is None:
        return None
    else:
        return sia.polarity_scores(string)['compound']


def predict_sentiment(df: pd.DataFrame):
    """
    Makes scores for each title and description and adds it as "score_description" and "score_title" to the Dataframe.
    """
    df['score_description'] = df['description'].apply(make_sentiment_score)
    df['score_title'] = df['title'].apply(make_sentiment_score)


def write_clean_news_to_bq(data: pd.DataFrame, table='clean_news', project_id='tomastestproject-433206', dataset='testdb_1'):
    """
    Writes cleaned data to Big Query
    """
    # Initiera BigQuery-klienten
    secret_data = get_secret()

    # Ladda JSON-strängen till en dictionary
    service_account_info = json.loads(secret_data)

    # Initiera BigQuery-klienten med service account
    client = bigquery.Client.from_service_account_info(
        service_account_info)

    # Definiera fullständigt tabell-id
    table_id = f"{project_id}.{dataset}.{table}"

    # Ladda DataFrame till BigQuery
    job = client.load_table_from_dataframe(data, table_id)

    # Vänta tills jobbet är klart
    job.result()

    # Kontrollera om det blev fel vid insättning av rader
    if job.errors:
        print(f"Errors: {job.errors}")
    else:
        return f'{job.output_rows} rader sparades till {table}'


def make_table(table_name='clean_news', project_id='tomastestproject-433206', database='testdb_1'):
    """
    Creates table for cleaned news in Big Query.
    """
    # Initiera BigQuery-klienten
    client = bigquery.Client.from_service_account_json(
        '/Users/tomasrydenstam/Desktop/Skola/test_project/transform_news/tomastestproject-433206-adc5bc090976.json'
    )

    table_id = f"{project_id}.{database}.{table_name}"

    # Definiera schema med uppdaterat kolumnnamn
    schema = [
        bigquery.SchemaField("author", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("pub_date", "TIMESTAMP"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("source_name", "STRING"),  # Uppdaterat kolumnnamn
        bigquery.SchemaField("company", "STRING"),
        bigquery.SchemaField("score_description", "FLOAT"),
        bigquery.SchemaField("score_title", "FLOAT"),
    ]

    # Skapa en Tabellreferens
    table = bigquery.Table(table_id, schema=schema)

    # Skapa Tabell
    table = client.create_table(table)  # Här skapas tabellen med table-objektet
    print(f"Created table {table_id}")
