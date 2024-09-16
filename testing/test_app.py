import os
#import pytest
from google.cloud import secretmanager
from google.auth import default
from app import get_project_id, get_secret


# def test_get_project_id_env_var():
#     # Sätt en miljövariabel för testet
#     os.environ['GOOGLE_CLOUD_PROJECT'] = 'test-project-id'
#     assert get_project_id() == 'test-project-id'


def test_get_project_id_default_credentials():
    # Ta bort miljövariabeln för att använda default credentials
    if 'GOOGLE_CLOUD_PROJECT' in os.environ:
        del os.environ['GOOGLE_CLOUD_PROJECT']

    # Ställ in miljövariabeln för default credentials
    # Anta att du har konfigurerat standard credential i din miljö
    _, project_id = default()
    assert get_project_id() == project_id


def test_get_secret():
    # Du måste se till att denna testmiljö har korrekt konfigurerad GitHub Secret
    # Byt till det faktiska hemlighetsnamnet du använder
    secret_name = 'bigquery-accout-secret'
    secret_data = get_secret(secret_name)

    assert secret_data is not None
    assert secret_data != ''


# def test_get_secret_error():
#     # För att simulera en felaktig Secret Manager-konfiguration,
#     # kan du konfigurera en hemlighet som inte finns eller är felaktig i din miljö.
#     secret_name = 'nonexistent-secret'  # Ett hemlighetsnamn som inte finns

#     with pytest.raises(Exception) as excinfo:
#         get_secret(secret_name)

#     assert 'Error fetching secret' in str(excinfo.value)
