import os
from google.cloud import secretmanager


def load_env_from_secret(secret_name: str, project_id: str):
    # Skapa en klient för Secret Manager
    client = secretmanager.SecretManagerServiceClient()

    # Bygg sökvägen till hemligheten
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    # Hämta och dekoda hemligheten
    response = client.access_secret_version(name=secret_path)
    secret_data = response.payload.data.decode('UTF-8')

    # Ladda miljövariabler från hemligheten
    for line in secret_data.splitlines():
        key, value = line.split('=', 1)
        os.environ[key] = value


# t.ex., os.system("your_application_command")
