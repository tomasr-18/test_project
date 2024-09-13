import pandas as pd
from google.cloud import bigquery
from nltk.sentiment import SentimentIntensityAnalyzer
from google.cloud import secretmanager
import json
from google.api_core.exceptions import GoogleAPIError, NotFound
import os
from google.auth import default


def get_project_id():
    """Retrieve project ID either from environment or default credentials."""
    # First, check if the GOOGLE_CLOUD_PROJECT env var is set
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')

    if not project_id:
        # If not set, retrieve the project ID from default credentials
        _, project_id = default()
    return project_id

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
    project_id = get_project_id()  # Ersätt med ditt projekt-ID
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Hämta den senaste versionen av hemligheten
    response = client.access_secret_version(name=secret_path)

    # Dekoda hemligheten till en sträng
    secret_data = response.payload.data.decode('UTF-8')

    return secret_data


def get_raw_news_from_big_query(raw_data_table:str,
                                meta_data_table:str, 
                                project_id:str, 
                                dataset:str
                                ):
    """
    Fetches unprocessed raw news data from a specified BigQuery table and returns it as a pandas DataFrame 
    along with a string of row IDs that were used.

    This function connects to Google BigQuery using credentials from a service account, constructs and executes 
    an SQL query to select rows where `is_processed` is FALSE from the specified table within the provided 
    dataset and project. The results are returned as a pandas DataFrame. It also extracts the `unique_id` from 
    the DataFrame and compiles them into a comma-separated string format for further processing.

    Args:
        table (str): The name of the table in BigQuery to fetch data from. Default is 'raw_news'.
        project_id (str): The Google Cloud project ID where the BigQuery dataset is located. Default is 'tomastestproject-433206'.
        dataset (str): The BigQuery dataset name that contains the table. Default is 'testdb_1'.

    Returns:
        pd.DataFrame: A DataFrame containing the unprocessed raw news data.
        str: A string of `unique_id`s from the fetched rows, formatted as comma-separated values in single quotes.

    Raises:
        ValueError: If no unprocessed data is found in the table.xw
    """
    secret_data = get_secret()

    # Ladda JSON-strängen till en dictionary
    service_account_info = json.loads(secret_data)

    # Initiera BigQuery-klienten med service account
    client = bigquery.Client.from_service_account_info(service_account_info)

    raw_data_table_id = f"{project_id}.{dataset}.{raw_data_table}"
    meta_data_table_id = f"{project_id}.{dataset}.{meta_data_table}"


    # Build your SQL query
    query = f"""
        SELECT unique_id, data,company
        FROM `{raw_data_table_id}`
        WHERE unique_id IN (SELECT unique_id FROM `{meta_data_table_id}` 
        WHERE is_processed IS FALSE)
        """

    # Execute the SQL query
    query_job = client.query(query)

    # Fetch the results
    results = query_job.result()

    # Convert results to a DataFrame
    df = results.to_dataframe()

    # Check if DataFrame is empty and raise an error if needed
    if df.empty:
        raise ValueError("No unprocessed data found")

    # Create a comma-separated string of unique IDs
    processed_id_list = df["unique_id"].to_list()
    id_str = ', '.join(f"'{id}'" for id in processed_id_list)

    return df, id_str


def update_is_processed(id_string: str,
                        table:str, 
                        project_id:str, 
                        dataset:str):

    table_id = f"{project_id}.{dataset}.{table}"
    secret_data = get_secret()

    # Ladda JSON-strängen till en dictionary
    service_account_info = json.loads(secret_data)

    # Initiera BigQuery-klienten med service account
    client = bigquery.Client.from_service_account_info(
        service_account_info)
   

        # Konstruera SQL-frågan
    query = f"""
    UPDATE `{table_id}`
    SET is_processed = TRUE
    WHERE unique_id IN ({id_string});
    """

    # Kör frågan
    job = client.query(query)
    job.result()  # Vänta på att jobbet ska slutföras
    return id_string


def clean_news(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms raw news data extracted from BigQuery into a structured DataFrame format.

    This function takes a DataFrame containing raw news data, unpacks JSON-like structures to separate rows 
    for each news article, and normalizes the data into a flat table format. The resulting DataFrame will have 
    one row per article with relevant information such as author, description, publication date, title, URL, 
    source, company, and sentiment scores.

    Args:
        df (pd.DataFrame): DataFrame containing raw news data with columns `unique_id`, `company`, `fetch_date`, `data`, and `is_processed`.

    Returns:
        pd.DataFrame: A DataFrame where each row represents a single news article with additional columns from the `company` column.
    """
    def extract_articles(json_obj):
        if isinstance(json_obj, dict) and 'articles' in json_obj:
            return json_obj['articles']
        return []

    # Förbered DataFrame
    df['data'] = df['data'].apply(
        lambda x: json.loads(x) if isinstance(x, str) else {})
    df['data'] = df['data'].apply(extract_articles)

    # Explodera artiklar till separata rader
    df_exploded = df.explode('data')

    # Normalisera JSON-data i 'data' kolumnen
    articles_df = pd.json_normalize(df_exploded['data'])

    # Lägg till övriga kolumner
    final_df = pd.concat(
        [articles_df, df_exploded[['company']].reset_index(drop=True)], axis=1)

    # Droppa eventuellt onödiga kolumner
    final_df.drop(columns=['content', 'source.id',
                  'urlToImage'], inplace=True, errors='ignore')

    # Omvandla 'publishedAt' till datetime format
    final_df['publishedAt'] = pd.to_datetime(
        final_df['publishedAt'], format='%Y-%m-%dT%H:%M:%SZ', utc=True, errors='coerce')

    # Omvandla kolumnnamn för läsbarhet
    final_df.rename(columns={"source.name": "source_name",
                    "publishedAt": "pub_date"}, inplace=True)

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


def write_clean_news_to_bq(data: pd.DataFrame, 
                           table='clean_news_copy', 
                           project_id='tomastestproject-433206', 
                           dataset='testdb_1'):
    """
    Writes cleaned data to Big Query
    """
    #Initiera BigQuery-klienten
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


def transfer_ids_to_meta_data(table_from:str, 
                              table_to:str, 
                              project_id:str, 
                              dataset:str, 
                              ):
    try:
        # Initiera BigQuery-klienten

        # Hämta JSON-sträng från Secret Manager
        secret_data = get_secret()

        # Ladda JSON-strängen till en dictionary
        service_account_info = json.loads(secret_data)

        # Initiera BigQuery-klienten med service account
        client = bigquery.Client.from_service_account_info(
            service_account_info)

        # Definiera din dataset och tabell
        meta_data_table = f"{project_id}.{dataset}.{table_to}"
        raw_data_table = f"{project_id}.{dataset}.{table_from}"
        query = f"""
                    INSERT INTO `{meta_data_table}` (unique_id, is_processed)
                    SELECT unique_id, FALSE
                    FROM `{raw_data_table}`
                    WHERE unique_id NOT IN (
                    SELECT unique_id
                    FROM `{meta_data_table}`)
                """
        # Infoga data till BigQuery
        errors = client.query(query)

        # Wait for the job to complete
        result = errors.result()

        # Retrieve the number of rows affected by the query
        rows_inserted = result.num_dml_affected_rows
        return rows_inserted
    except NotFound:
        print(f"Error: The table {meta_data_table} was not found.")
        raise

    except GoogleAPIError as e:
        print(f"Google API Error: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

