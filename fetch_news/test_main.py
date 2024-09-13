
import os
import pytest
from fastapi.testclient import TestClient
from main import app
from unittest.mock import patch

client = TestClient(app)


@pytest.fixture(scope="module", autouse=True)
def setup_environment():
    # Set environment variables from GitHub Actions secrets
    # Path to the JSON key file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your/gcp-key.json"
    # Replace with your API key from GitHub secrets
    os.environ["NEWS_API_KEY"] = "your_news_api_key"


def test_fetch_news_and_save():
    # Mock fetch_news and save_raw_data_to_big_query functions to avoid actual external calls
    with patch('main.fetch_news') as mock_fetch_news, \
            patch('main.save_raw_data_to_big_query') as mock_save_raw_data_to_big_query:

        mock_fetch_news.return_value = {
            'articles': [{'title': 'Test Article', 'description': 'Test Description'}],
            'totalResults': 1
        }
        mock_save_raw_data_to_big_query.return_value = None

        response = client.post("/fetch-news/", json={
            "company": "Test Company",
            "from_date": "2023-01-01",
            "to_date": "2023-01-02",
            "table_name": "test_table"
        })

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["message"] == "Data fetched and saved successfully."
        assert response_json["Number of articels saved: "] == 1
        assert response_json["company"] == "Test Company"
