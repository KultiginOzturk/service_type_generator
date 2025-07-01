import os
from google.cloud import bigquery
from google.oauth2 import service_account
from utils.logger import Logger

logger = Logger(__name__)

def get_bq_client():
    # Path to your service-account JSON key.
    # By default GOOGLE_APPLICATION_CREDENTIALS points here,
    # but you can hard-code a path if you prefer.
    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "path/to/key.json")

    # We need both BigQuery AND Drive scopes
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/drive",
    ]

    # Build credentials with those scopes
    creds = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=scopes,
    )

    logger.info("Creating BigQuery client")
    # Return a client that carries both scopes
    return bigquery.Client(
        credentials=creds,
        project=creds.project_id,
    )
