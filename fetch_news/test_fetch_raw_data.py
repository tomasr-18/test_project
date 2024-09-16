# # import os
# # import pytest
# # from unittest.mock import patch, Mock
# # from fetch_raw_data import get_project_id, fetch_news
# # import requests


# # def test_get_project_id_with_env_var():
# #     # Testar att projekt-ID hämtas korrekt från miljövariabler
# #     with patch.dict(os.environ, {"GOOGLE_CLOUD_PROJECT": "test-project"}):
# #         project_id = get_project_id()
# #         assert project_id == "test-project"


# # @pytest.fixture
# # def mock_requests_get():
# #     with patch('requests.get') as mock_get:
# #         yield mock_get


# # def test_fetch_news_success(mock_requests_get):
# #     # Mockar ett lyckat API-svar
# #     mock_response = Mock()
# #     mock_response.status_code = 200
# #     mock_response.json.return_value = {
# #         "status": "ok",
# #         "articles": [{"title": "Test Article", "description": "Test Description"}]
# #     }
# #     mock_requests_get.return_value = mock_response

# #     result = fetch_news(
# #         company="Test Company",
# #         api_key="fake-api-key",
# #         from_date="2023-01-01",
# #         to_date="2023-01-02"
# #     )

# #     assert result["status"] == "ok"
# #     assert len(result["articles"]) == 1
# #     assert result["articles"][0]["title"] == "Test Article"


# # def test_fetch_news_api_error(mock_requests_get):
# #     # Mockar ett API-svar med fel
# #     mock_response = Mock()
# #     mock_response.status_code = 200
# #     mock_response.json.return_value = {
# #         "status": "error",
# #         "message": "API Key invalid."
# #     }
# #     mock_requests_get.return_value = mock_response

# #     with pytest.raises(ValueError, match="API Error: API Key invalid."):
# #         fetch_news(
# #             company="Test Company",
# #             api_key="fake-api-key",
# #             from_date="2023-01-01",
# #             to_date="2023-01-02"
# #         )


# # def test_fetch_news_network_error(mock_requests_get):
# #     # Simulerar ett nätverksfel
# #     mock_requests_get.side_effect = requests.exceptions.RequestException(
# #         "Network error")

# #     with pytest.raises(requests.exceptions.RequestException, match="Network error"):
# #         fetch_news(
# #             company="Test Company",
# #             api_key="fake-api-key",
# #             from_date="2023-01-01",
# #             to_date="2023-01-02"
# #         )


# import os
# # import pytest
# # from google.cloud import secretmanager
# from google.auth import default
# from fetch_raw_data import get_project_id, get_secret


# # def test_get_project_id_env_var():
# #     # Sätt en miljövariabel för testet
# #     os.environ['GOOGLE_CLOUD_PROJECT'] = 'test-project-id'
# #     assert get_project_id() == 'test-project-id'


# def test_get_project_id_default_credentials():
#     # Ta bort miljövariabeln för att använda default credentials
#     if 'GOOGLE_CLOUD_PROJECT' in os.environ:
#         del os.environ['GOOGLE_CLOUD_PROJECT']

#     # Ställ in miljövariabeln för default credentials
#     # Anta att du har konfigurerat standard credential i din miljö
#     _, project_id = default()
#     assert get_project_id() == project_id


# def test_get_secret():
#     # Du måste se till att denna testmiljö har korrekt konfigurerad GitHub Secret   
#     # Byt till det faktiska hemlighetsnamnet du använder
#     secret_name = 'bigquery-accout-secret'
#     secret_data = get_secret(secret_name)

#     assert secret_data is not None
#     assert secret_data != ''


# # def test_get_secret_error():
# #     # För att simulera en felaktig Secret Manager-konfiguration,
# #     # kan du konfigurera en hemlighet som inte finns eller är felaktig i din miljö.
# #     secret_name = 'nonexistent-secret'  # Ett hemlighetsnamn som inte finns

# #     with pytest.raises(Exception) as excinfo:
# #         get_secret(secret_name)

# #     assert 'Error fetching secret' in str(excinfo.value)
