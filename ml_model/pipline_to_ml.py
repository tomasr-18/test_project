import pandas as pd
import json
from google.cloud import bigquery
from google.cloud import secretmanager
from google.api_core.exceptions import GoogleAPICallError, NotFound, BadRequest
from typing import List


def get_secret(secret_name='bigquery-accout-secret') -> str:
    """Fetches a secret from Google Cloud Secret Manager.

    Args:
        secret_name (str): The name of the secret in Secret Manager.

    Returns:
        str: The secret data as a string.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        project_id = 'tomastestproject-433206'  # Replace with your project ID
        secret_path = f"projects/{project_id}/secrets/{
            secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        secret_data = response.payload.data.decode('UTF-8')
        return secret_data
    except NotFound:
        raise Exception(
            f"Secret '{secret_name}' not found in project '{project_id}'.")
    except GoogleAPICallError as e:
        raise Exception(f"Failed to access secret '{secret_name}': {e}")


def get_data_by_company(company: List[str],
                        project_id='tomastestproject-433206',
                        dataset='testdb_1',
                        table='avg_scores_and_stock_data_right') -> pd.DataFrame:
    """
    Retrieves data from a specified BigQuery table based on a list of company names.

    :param company: List of company names (strings) to filter the data.
    :param project_id: Google Cloud project ID where the BigQuery dataset is located.
                       Default is 'tomastestproject-433206'.
    :param dataset: BigQuery dataset name. Default is 'testdb_1'.
    :param table: BigQuery table name. Default is 'avg_scores_and_stock_data'.
    :return: A pandas DataFrame containing the query results filtered by the specified companies.
    """
    try:
        secret_data = get_secret()
        service_account_info = json.loads(secret_data)
        client = bigquery.Client.from_service_account_info(
            service_account_info)

        view_id = f"{project_id}.{dataset}.{table}"
        company_str = ", ".join([f"'{c}'" for c in company])
        query = f"""
            SELECT *
            FROM `{view_id}`
            WHERE company IN ({company_str})
        """

        query_job = client.query(query)
        results = query_job.result()
        df = results.to_dataframe()

        if df.empty:
            print("Warning: The query returned no results.")
            return pd.DataFrame()  # Return an empty DataFrame
        return df

    except GoogleAPICallError as e:
        raise Exception(f"BigQuery API call failed: {e}")
    except BadRequest as e:
        raise Exception(f"Query syntax error: {e}")
    except json.JSONDecodeError:
        raise Exception("Failed to decode service account info from secret.")
    except Exception as e:
        raise Exception(f"Failed to retrieve data from BigQuery: {e}")


def calculate_rolling_average(df: pd.DataFrame, column_name: str, window_size=5) -> pd.DataFrame:
    """
    Calculates a rolling average over the last n days for a specified column,
    grouped by company and ordered by date.

    :param df: DataFrame containing the data
    :param column_name: The name of the column for which the rolling average should be calculated
    :param window_size: Number of days over which the rolling average is calculated (default is 5)
    :return: DataFrame with a new column for the rolling average
    """
    try:
        if column_name not in df.columns:
            raise ValueError(
                f"Column '{column_name}' does not exist in the DataFrame.")

        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        if df['pub_date'].isnull().any():
            raise ValueError("Invalid date format in 'pub_date' column.")

        df = df.sort_values(by=['company', 'pub_date'])
        rolling_avg_column_name = f'rolling_avg_{column_name}'
        df[rolling_avg_column_name] = df.groupby('company')[column_name].transform(
            lambda x: x.rolling(window=window_size, min_periods=1).mean()
        )
        df[rolling_avg_column_name] = df[rolling_avg_column_name].fillna(
            df[column_name])

        return df

    except KeyError as e:
        raise Exception(f"Missing key in DataFrame: {e}")
    except Exception as e:
        raise Exception(f"Failed to calculate rolling average: {e}")


def transform_data_to_model(df: pd.DataFrame, from_date="2024-08-02") -> pd.DataFrame:
    """
    Transforms the input DataFrame by adding a target column, filtering by date, and filling missing values.

    :param df: DataFrame containing the data.
    :param from_date: The start date for filtering the DataFrame.
    :return: Transformed DataFrame with added target column, filtered rows, and filled missing values.
    """
    try:
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        if df['pub_date'].isnull().any():
            raise ValueError("Invalid date format in 'pub_date' column.")

        df_right_dates = df[df["pub_date"] >= from_date]

        def add_target_column(df: pd.DataFrame):
            df = df.sort_values(by=['company', 'pub_date'])
            df['target'] = df.groupby('company')['close'].shift(-1)
            df['target'] = df['target'].fillna(df['close'])
            return df

        df_with_target = add_target_column(df=df_right_dates)
        df_with_target.fillna(0, inplace=True)

        return df_with_target

    except KeyError as e:
        raise Exception(f"Missing expected column in DataFrame: {e}")
    except Exception as e:
        raise Exception(f"Failed to transform data to model format: {e}")


def save_predictions_to_big_query(data: pd.DataFrame,
                                  project_id='tomastestproject-433206',
                                  dataset='testdb_1',
                                  table="predictions"):
    """
    Saves a DataFrame to a specified BigQuery table.

    :param data: DataFrame containing the data to be saved.
    :param project_id: Google Cloud project ID where the BigQuery dataset is located.
    :param dataset: BigQuery dataset name.
    :param table: BigQuery table name.
    """
    try:
        secret_data = get_secret()
        service_account_info = json.loads(secret_data)
        client = bigquery.Client.from_service_account_info(
            service_account_info)

        table_id = f"{project_id}.{dataset}.{table}"
        job = client.load_table_from_dataframe(data, table_id)
        job.result()  # Wait for the job to complete

        if job.errors:
            raise Exception(f"Errors during BigQuery load: {job.errors}")
        else:
            return (f'{job.output_rows} rows saved to {table_id}')

    except GoogleAPICallError as e:
        raise Exception(f"BigQuery API call failed: {e}")
    except json.JSONDecodeError:
        raise Exception("Failed to decode service account info from secret.")
    except Exception as e:
        raise Exception(f"Failed to save predictions to BigQuery: {e}")
