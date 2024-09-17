import unittest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient
from fetch_stocks_raw import app, get_secret, fetch_raw_stock_data, save_raw_stock_data
import json

client = TestClient(app)

class TestFetchStocksRaw(unittest.TestCase):

    @patch('google.auth.default', return_value=(None, 'mock_project_id'))  # Mock google.auth.default to return mock project ID
    @patch('fetch_stocks_raw.secretmanager.SecretManagerServiceClient')
    @patch('fetch_stocks_raw.get_project_id', return_value='mock_project_id')  # Mock the project ID
    def test_get_secret(self, mock_get_project_id, mock_secret_manager_client, mock_google_auth):
        # Mock the secret manager client
        mock_client_instance = mock_secret_manager_client.return_value
        mock_client_instance.access_secret_version.return_value.payload.data.decode.return_value = 'mock_secret'

        secret = get_secret('mock_secret_name')
        self.assertEqual(secret, 'mock_secret')

    @patch('fetch_stocks_raw.requests.get')
    @patch('fetch_stocks_raw.get_stock_api_key', return_value='mock_api_key')  # Mock the API key fetching function
    def test_fetch_raw_stock_data(self, mock_get_stock_api_key, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {"Time Series (Daily)": {"2023-01-01": {"1. open": "100.0"}}}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        stock_data = fetch_raw_stock_data('AAPL')
        self.assertIn("Time Series (Daily)", stock_data)

    @patch('fetch_stocks_raw.bigquery.Client')
    @patch('fetch_stocks_raw.service_account.Credentials.from_service_account_info')
    @patch('fetch_stocks_raw.get_secret')
    @patch('fetch_stocks_raw.get_project_id', return_value='mock_project_id')  # Mock the project ID for BigQuery
    def test_save_raw_stock_data(self, mock_get_project_id, mock_get_secret, mock_credentials, mock_bigquery_client):
        # Mock the secret and BigQuery client
        mock_get_secret.return_value = '{"type": "service_account"}'
        mock_client_instance = mock_bigquery_client.return_value
        mock_client_instance.create_dataset.return_value = None  # Simulate successful dataset creation
        mock_client_instance.table.return_value.exists.return_value = True  # Simulate existing table
        mock_client_instance.insert_rows_json.return_value = []  # Simulate successful data insertion

        stock_data = {"Time Series (Daily)": {"2023-01-01": {"1. open": "100.0"}}}
        response = save_raw_stock_data('AAPL', stock_data, 'mock_table_id')
        self.assertEqual(response.status_code, 200)
        response_content = json.loads(response.body.decode())
        self.assertIn("Rows successfully inserted.", response_content["message"])

    @patch('fetch_stocks_raw.fetch_raw_stock_data')
    @patch('fetch_stocks_raw.save_raw_stock_data')
    def test_handle_raw_stock_data(self, mock_save_raw_stock_data, mock_fetch_raw_stock_data):
        # Mock the fetch and save functions
        mock_fetch_raw_stock_data.return_value = {"Time Series (Daily)": {"2023-01-01": {"1. open": "100.0"}}}
        mock_save_raw_stock_data.return_value = MagicMock(status_code=200, body=json.dumps({"message": "Rows successfully inserted."}).encode())

        response = client.post("/raw-stock-data/", json={"stock_symbol": "AAPL"})
        self.assertEqual(response.status_code, 200)
        response_content = json.loads(response.content)
        self.assertIn("Rows successfully inserted.", response_content["message"])

if __name__ == "__main__":
    unittest.main()