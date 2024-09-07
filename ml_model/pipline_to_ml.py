import pandas as pd
import json
from google.cloud import bigquery
from google.cloud import secretmanager
from google.api_core.exceptions import GoogleAPICallError, NotFound, BadRequest
from typing import List
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression


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
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
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
        # Konvertera 'pub_date' till datetime och hantera fel
        df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')
        if df['pub_date'].isnull().any():
            raise ValueError("Invalid date format in 'pub_date' column.")

        # Filtrera DataFrame baserat på 'from_date'
        df_right_dates = df[df["pub_date"] >= from_date]

        def add_target_column(df: pd.DataFrame) -> pd.DataFrame:
            # Sortera DataFrame efter 'company' och 'pub_date'
            df = df.sort_values(by=['company', 'pub_date'])

            # Skapa 'target'-kolumn genom att skifta 'close' värdet för nästa datum per 'company'
            df['target'] = df.groupby('company')['close'].shift(-1)

            # Fyll endast NaN i den första raden i varje grupp (inte den sista)
            def fill_first_na(group: pd.DataFrame) -> pd.DataFrame:
                if pd.isna(group['target'].iloc[0]):
                    group.loc[group.index[0],
                              'target'] = group['close'].iloc[0]
                return group

            # Använd funktionen på varje grupp och återställ indexet
            df = df.groupby('company').apply(
                fill_first_na).reset_index(drop=True)
            return df

        df_with_target = add_target_column(df=df_right_dates)

        # Fyll NaN i specifika kolumner
        df_with_target[['avg_score_description', 'avg_score_title']] = df_with_target[[
            'avg_score_description', 'avg_score_title']].fillna(0)

        return df_with_target

    except KeyError as e:
        raise Exception(f"Missing expected column in DataFrame: {e}")
    except Exception as e:
        raise Exception(f"Failed to transform data to model format: {e}")

    except KeyError as e:
        raise Exception(f"Missing expected column in DataFrame: {e}")
    except Exception as e:
        raise Exception(f"Failed to transform data to model format: {e}")


def save_predictions_to_big_query(data: list,
                                  project_id='tomastestproject-433206',
                                  dataset='testdb_1',
                                  table='predictions'):
    """
    Saves a list of dictionaries to a specified BigQuery table.

    :param data: List of dictionaries containing the data to be saved.
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

        # Insert rows into BigQuery
        errors = client.insert_rows_json(table_id, data)

        if errors:
            raise Exception(f"Errors during BigQuery load: {errors}")
        else:
            return f'{len(data)} rows saved to {table_id}'

    except GoogleAPICallError as e:
        raise Exception(f"BigQuery API call failed: {e}")
    except json.JSONDecodeError:
        raise Exception("Failed to decode service account info from secret.")
    except Exception as e:
        raise Exception(f"Failed to save predictions to BigQuery: {e}")


def train_model(df: pd.DataFrame, company_list=['AAPL', 'GOOGL', 'TSLA', 'AMZN', 'MSFT']) -> dict:
    model_dict = {}
    prediction_rows = {}
    for company in company_list:

        company_df = df[df["company"] == company]
        company_df_sorted = company_df.sort_values(by="pub_date")

        date = company_df_sorted["pub_date"].tail(1)

        y = company_df_sorted['target']
        X = company_df_sorted.drop(columns=["company", "target", "pub_date"])

        X_scaled = scale_features(df=X)

        prediction_row = X_scaled.tail(1)
        prediction_rows[f'{company}'] = prediction_row

        X_train = X_scaled.iloc[:-1]
        y_train = y.iloc[:-1]

        model = LinearRegression()
        model.fit(X_train, y_train)
        model_dict[f'{company}_model'] = model

    return model_dict, prediction_rows, date.iloc[0].strftime('%Y-%m-%dT%H:%M:%S')


def scale_features(df: pd.DataFrame, features_to_scale=['open',	'high',	'low', 'close', 'volume', 'rolling_avg_close']) -> pd.DataFrame:
    """
    Skalar endast de angivna features i DataFrame.

    :param x_df: DataFrame med alla features
    :param features_to_scale: Lista med kolumnnamn som ska skalas
    :return: DataFrame med skalade och icke-skalade features
    """
    # Skapa en kopia av DataFrame för att undvika att ändra originaldatan
    df_scaled = df.copy()

    # Initiera skalaren
    scaler = StandardScaler()

    # Skala endast de specifika kolumnerna
    df_scaled[features_to_scale] = scaler.fit_transform(
        df[features_to_scale])

    return df_scaled


def make_prediction(models_dict: dict, x_pred: dict) -> dict:
    predictions_by_company = {}
    for model, prediction_row in zip(models_dict.items(), x_pred.items()):
        reg_model = model[1]
        company = prediction_row[0]
        df = prediction_row[1]
        pred = reg_model.predict(df)
        predictions_by_company[company] = pred
    return predictions_by_company


def transform_predictions_for_bq(predictions: dict, date: str) -> list:
    to_bq = [
        {
            "company": company,
            # Vi antar att värdet alltid är en array med ett element
            "predicted_value": float(value[0]),
            "date": date
        }
        for company, value in predictions.items()
    ]
    return to_bq


def save_models():
    pass
