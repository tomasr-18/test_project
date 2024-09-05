import functions_framework
import json
from google.cloud import bigquery, secretmanager
from google.api_core.exceptions import NotFound
from pydantic import BaseModel, Field
from typing import Dict, Any

# Definiera en modell för förfrågning


class RequestModel(BaseModel):
    table_name: str
    table_type: str
    project_id: str = Field(default="tomastestproject-433206")
    dataset_id: str = Field(default="testdb_1")


@functions_framework.http
def create_bigquery_table(request) -> Dict[str, Any]:
    try:
        # Läsa JSON-data från förfrågan
        data = RequestModel.parse_raw(request.get_data())
    except ValueError as e:
        return {"error": f"Invalid request payload: {e}"}, 400

    table_name = data.table_name
    table_type = data.table_type
    project_id = data.project_id
    dataset_id = data.dataset_id

    def get_secret(secret_name='bigquery-accout-secret') -> str:
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(name=secret_path)
        secret_data = response.payload.data.decode('UTF-8')
        return secret_data

    secret_data = get_secret()
    service_account_info = json.loads(secret_data)
    client = bigquery.Client.from_service_account_info(service_account_info)

    valid_table_types = ["clean_news_data", "clean_stock_data",
                         "raw_news_data", "raw_news_meta_data", "raw_stock_data"]

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
        return {"error": f"Invalid table_type '{table_type}'"}, 400

    table_id = f"{project_id}.{dataset_id}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)
        return {"message": f"Table {table_id} already exists."}, 200
    except NotFound:
        try:
            client.create_table(table)
            return {"message": f"Table {table_id} created successfully."}, 200
        except Exception as e:
            return {"error": f"An error occurred while creating the table: {e}"}, 500
