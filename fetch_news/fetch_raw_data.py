from google.api_core.exceptions import GoogleAPIError, NotFound
import requests
from google.cloud import bigquery
from datetime import datetime
import json
from google.cloud import secretmanager
import uuid
from google.auth import default
import os

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

def fetch_news(company: str, api_key: str,
               from_date: str,
               to_date: str,
             sort_by: str = 'relevance',
             language: str = 'en') -> dict:
    """
    Hämtar nyhetsdata för ett specifikt företag från NewsAPI.
    
    Args:
        company (str): Namnet på företaget som ska sökas efter.
        api_key (str): API-nyckeln för att autentisera mot NewsAPI.
        from_date (str): Startdatum för sökningen (format 'YYYY-MM-DD')
        to_date (str): Slutdatum för sökningen (format 'YYYY-MM-DD')
        sort_by (str): Sorteringskriterium (t.ex. 'relevance'). Standard är 'relevance'.
        language (str): Språk för nyheterna. Standard är 'en'.
    
    Returns:
        dict: JSON-svar från NewsAPI om anropet lyckas.
    
    Raises:
        ValueError: Om API-svaret innehåller ett felmeddelande.
        requests.exceptions.RequestException: Om nätverksfel eller anslutningsfel uppstår.
    """
    url = f'https://newsapi.org/v2/everything?q={company}&from={from_date}&to={to_date}&sortBy={sort_by}&language={language}&apiKey={api_key}'

    try:
        response = requests.get(url=url)
        response.raise_for_status()  # Kontrollera för HTTP-fel
        data = response.json()

        # Kontrollera om API-anropet innehåller fel
        if data.get("status") != "ok":
            raise ValueError(
                f"API Error: {data.get('message', 'Unknown error')}")

        return data

    except requests.exceptions.RequestException as e:
        print(f"Nätverksfel eller anslutningsproblem: {e}")
        raise

    except ValueError as ve:
        print(f"Fel i API-svaret: {ve}")
        raise


def save_raw_data_to_big_query(data: dict, 
                               company: str, 
                               table='raw_news', 
                               project_id=get_project_id(),     
                               dataset= get_secret('dataset'), 
                               secret='bigquery-accout-secret'):
    """
    Sparar rådata till BigQuery med datum och företagsnamn.

    Args:
        data (dict): Rådata som ska sparas.
        company (str): Företagsnamn som en kolumn.
        table (str): Namnet på tabellen att spara i.
        project_id (str): Google Cloud Project ID.
        dataset (str): Namnet på datasetet.

    Raises:
        GoogleAPIError: Vid fel med BigQuery.
        Exception: Vid andra fel under insättningen.
    """
    try:
        # Initiera BigQuery-klienten

        # Hämta JSON-sträng från Secret Manager
        secret_data = get_secret(secret)

        # Ladda JSON-strängen till en dictionary
        service_account_info = json.loads(secret_data)

        # Initiera BigQuery-klienten med service account
        client = bigquery.Client.from_service_account_info(
            service_account_info)
    

        # Lägg till dagens datum i data-dict
        fetch_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Förbered data för BigQuery
        rows_to_insert = [{
            "data": json.dumps(data),  # JSON-sträng för din data
            "fetch_date": fetch_date,  # Datum som en separat kolumn
            "company": company,
            "unique_id": str(uuid.uuid4())
        }]

        # Definiera din dataset och tabell
        table_id = f"{project_id}.{dataset}.{table}"
        #print(f"Table ID: {table_id}")

        # Infoga data till BigQuery
        errors = client.insert_rows_json(table_id, rows_to_insert)

        # Kontrollera om det blev fel vid infogning
        if errors:
            print(f"Errors: {errors}")
            raise RuntimeError(f"Failed to insert rows: {errors}")
        else:
            print("Data successfully inserted.")

    except NotFound:
        print(f"Error: The table {table_id} was not found.")
        raise

    except GoogleAPIError as e:
        print(f"Google API Error: {e}")
        raise

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


if __name__=='__main__':
    #print(get_secret())
    pass