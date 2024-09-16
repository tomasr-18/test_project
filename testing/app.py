from google.cloud import secretmanager
import os
from google.auth import default


def get_project_id():
    """Retrieve project ID either from environment or default credentials."""
    # First, check if the GOOGLE_CLOUD_PROJECT env var is set
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')

    if not project_id:
        # If not set, retrieve the project ID from default credentials
        _, project_id = default()
    return project_id

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
    project_id = get_project_id()  # Ersätt med ditt projekt-ID
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Hämta den senaste versionen av hemligheten
    response = client.access_secret_version(name=secret_path)

    # Dekoda hemligheten till en sträng
    secret_data = response.payload.data.decode('UTF-8')

    return secret_data
