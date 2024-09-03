import pandas as pd
import json
from google.cloud import bigquery
from google.cloud import secretmanager


from google.cloud import bigquery
import pandas as pd


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

def get_data_by_company(company: list[str],
                        project_id='tomastestproject-433206',
                        dataset='testdb_1',
                        table='avg_scores_and_stock_data'
                        ) -> pd.DataFrame:
    """
    Retrieves data from a specified BigQuery table based on a list of company names.

    :param company: List of company names (strings) to filter the data.
    :param project_id: Google Cloud project ID where the BigQuery dataset is located. 
                       Default is 'tomastestproject-433206'.
    :param dataset: BigQuery dataset name. Default is 'testdb_1'.
    :param table: BigQuery table name. Default is 'avg_scores_and_stock_data'.
    :return: A pandas DataFrame containing the query results filtered by the specified companies.
    """
    secret_data = get_secret()

    # Ladda JSON-strängen till en dictionary
    service_account_info = json.loads(secret_data)

    # Initialize BigQuery client using a service account JSON file for authentication
    client = bigquery.Client.from_service_account_info(service_account_info)
   

    # Define the full table ID in the format 'project.dataset.table'
    view_id = f"{project_id}.{dataset}.{table}"

    # Convert the list of companies into a string format suitable for SQL IN clause
    company_str = ", ".join([f"'{c}'" for c in company])

    # Build the SQL query to select data for the specified companies
    query = f"""
        SELECT *
        FROM `{view_id}`
        WHERE company IN ({company_str})
        """

    # Execute the SQL query
    query_job = client.query(query)

    # Fetch the results of the query
    results = query_job.result()

    # Convert the query results into a pandas DataFrame
    df = results.to_dataframe()

    # Return the DataFrame with the filtered data
    return df



def calculate_rolling_average(df:pd.DataFrame, column_name:str, window_size=5)->pd.DataFrame:
    """
    Calculates a rolling average over the last n days for a specified column.

    :param df: DataFrame containing the data
    :param column_name: The name of the column for which the rolling average should be calculated
    :param window_size: Number of days over which the rolling average is calculated (default is 5)
    :return: DataFrame with a new column for the rolling average
    """
    # Check that the column exists in the DataFrame
    if column_name not in df.columns:
        raise ValueError(
            f"Column '{column_name}' does not exist in the DataFrame.")

    # Sort the DataFrame by date
    df = df.sort_values(by='pub_date')

    # Add a new column with the rolling average
    df[f'rolling_avg_{column_name}'] = df[column_name].rolling(
        window=window_size).mean()

    return df
