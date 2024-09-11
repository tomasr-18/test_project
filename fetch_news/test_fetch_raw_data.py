import pytest
from unittest.mock import patch, MagicMock
from fetch_raw_data import get_secret, fetch_news, save_raw_data_to_big_query

# Test get_secret function


@patch('fetch_raw_data.secretmanager.SecretManagerServiceClient')
def test_get_secret(mock_secret_manager_client):
    mock_secret = MagicMock()
    mock_secret.payload.data.decode.return_value = 'test_secret'
    mock_secret_manager_client.return_value.access_secret_version.return_value = mock_secret

    secret = get_secret('test_secret_name')

    assert secret == 'test_secret'
    mock_secret_manager_client.assert_called_once()
    mock_secret_manager_client.return_value.access_secret_version.assert_called_once_with(
        name='projects/tomastestproject-433206/secrets/test_secret_name/versions/latest')

# Test fetch_news function with successful response


@patch('requests.get')
def test_fetch_news_success(mock_requests_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "ok", "articles": []}
    mock_requests_get.return_value = mock_response

    result = fetch_news('company_name', 'api_key', '2024-01-01', '2024-01-31')

    assert result == {"status": "ok", "articles": []}
    mock_requests_get.assert_called_once_with(
        url='https://newsapi.org/v2/everything?q=company_name&from=2024-01-01&to=2024-01-31&sortBy=relevance&language=en&apiKey=api_key'
    )

# Test fetch_news function with failed response


@patch('requests.get')
def test_fetch_news_failure(mock_requests_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "status": "error", "message": "error_message"}
    mock_requests_get.return_value = mock_response

    with pytest.raises(ValueError):
        fetch_news('company_name', 'api_key', '2024-01-01', '2024-01-31')

# Test save_raw_data_to_big_query function


@patch('fetch_raw_data.get_secret')
@patch('fetch_raw_data.bigquery.Client')
def test_save_raw_data_to_big_query_success(mock_bigquery_client, mock_get_secret):
    mock_secret = '{"private_key": "key"}'
    mock_get_secret.return_value = mock_secret

    mock_client = MagicMock()
    mock_bigquery_client.from_service_account_info.return_value = mock_client

    mock_client.insert_rows_json.return_value = []

    data = {"key": "value"}
    save_raw_data_to_big_query(data, 'company_name')

    mock_get_secret.assert_called_once_with('bigquery-accout-secret')
    mock_bigquery_client.from_service_account_info.assert_called_once()
    mock_client.insert_rows_json.assert_called_once()


@patch('fetch_raw_data.get_secret')
@patch('fetch_raw_data.bigquery.Client')
def test_save_raw_data_to_big_query_failure(mock_bigquery_client, mock_get_secret):
    mock_secret = '{"private_key": "key"}'
    mock_get_secret.return_value = mock_secret

    mock_client = MagicMock()
    mock_bigquery_client.from_service_account_info.return_value = mock_client

    mock_client.insert_rows_json.return_value = [{'errors': 'error'}]

    data = {"key": "value"}

    with pytest.raises(RuntimeError):
        save_raw_data_to_big_query(data, 'company_name')
