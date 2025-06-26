import os
from config import DATASET_ID, MERGED_SERVICE_TYPE_TABLE

SERVICE_TYPES_TABLE = os.getenv(
    "SERVICE_TYPES_TABLE", f"{DATASET_ID}.kulti_service_types"
)


def get_service_types_for_client(bq_client, client_id):
    query = f"""
        SELECT
            CAST(TYPE_ID as INT64) as TYPE_ID,
            DESCRIPTION,
            SAFE_CAST(RESERVICE AS INT64) as API_RESERVICE,
            SAFE_CAST(REGULAR_SERVICE AS INT64) as API_REGULAR_SERVICE,
            SAFE_CAST(FREQUENCY AS INT64) as API_FREQUENCY,
            SAFE_CAST(DEFAULT_LENGTH AS INT64) as API_DEFAULT_LENGTH,
            CLIENT as clientId
        FROM `{SERVICE_TYPES_TABLE}`
        WHERE CLIENT = '{client_id}'
    """
    print(f"Fetching service types for client: {client_id}")
    return bq_client.query(query).to_dataframe()


def get_merged_service_types_for_client(bq_client, client_id):
    query = f"""
        SELECT
            CAST(typeID as INT64) as TYPE_ID,
            description as DESCRIPTION,
            clientID as clientId
        FROM `{MERGED_SERVICE_TYPE_TABLE}`
        WHERE clientID = '{client_id}'
    """
    print(f"Fetching merged service types for client: {client_id}")
    return bq_client.query(query).to_dataframe()
