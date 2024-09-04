from google.cloud import bigquery, secretmanager ,NotFound
import json


def create_bigquery_table(table_name: str, table_type: str,project_id = 'tomastestproject-433206', dataset_id='testdb_1'):
    """
    Creates a BigQuery table with a specific schema if it does not already exist.

    Args:
        service_account_path (str): Path to the service account JSON file.
        project_id (str): GCP project ID.
        dataset_id (str): BigQuery dataset ID.
        table_name (str): Name of the table to be created.
        table_type (str): Type of the table which determines the schema to be used. Should be one of
                          "clean_news_data", "clean_stock_data", "raw_news_data", "raw_news_meta_data",
                          or "raw_stock_data".

    Returns:
        None
    """
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
    secret_data = get_secret()

    # Ladda JSON-strängen till en dictionary
    service_account_info = json.loads(secret_data)

    # Initiera BigQuery-klienten med service account
    client = bigquery.Client.from_service_account_info(
        service_account_info)
    
    valid_table_types = ["clean_news_data", "clean_stock_data",
                         "raw_news_data", "raw_news_meta_data", "raw_stock_data"]

    # Define schema based on the table type
    if table_type.lower() == valid_table_types[0]:
        schema = [
            bigquery.SchemaField("author", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pub_date", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("score_description",
                                 "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("score_title", "FLOAT", mode="NULLABLE"),
        ]
    elif table_type.lower() == valid_table_types[1]:
        schema = [
            bigquery.SchemaField("stock_symbol", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("open", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("high", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("low", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("close", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("volume", "INTEGER", mode="NULLABLE"),
        ]
    elif table_type.lower() == valid_table_types[2]:
        schema = [
            bigquery.SchemaField("data", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("fetch_date", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("unique_id", "STRING", mode="NULLABLE"),
        ]
    elif table_type.lower() == valid_table_types[3]:
        schema = [
            bigquery.SchemaField("unique_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("is_processed", "BOOLEAN", mode="REQUIRED"),
        ]
    elif table_type.lower() == valid_table_types[4]:
        schema = [
            bigquery.SchemaField("stock_symbol", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("raw_data", "JSON", mode="NULLABLE"),
            bigquery.SchemaField("fetch_date", "TIMESTAMP", mode="NULLABLE"),
        ]
    else:
        raise ValueError(f"Invalid table_type '{table_type}'. Must be one of {', '.join(valid_table_types)}.")

   

    # Define the table ID
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    # Create a table reference and pass the schema
    table = bigquery.Table(table_id, schema=schema)

    try:
            client.get_table(table_id)
            return f"Table {table_id} already exists.", 200
    except NotFound:
        try:
            # Create the table in BigQuery
            table = client.create_table(table)  # API request
            return f"Table {table_id} created successfully.", 200
        except Exception as e:
            return f"An error occurred while creating the table: {e}", 500


if __name__ == "__main__":
    pass
