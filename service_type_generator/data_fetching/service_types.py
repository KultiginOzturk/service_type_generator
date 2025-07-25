import os
from config import RAW_DATASET_ID, MERGED_SERVICE_TYPE_TABLE
from utils.logger import Logger

logger = Logger(__name__)

SERVICE_TYPES_TABLE = os.getenv(
    "SERVICE_TYPES_TABLE", f"{RAW_DATASET_ID}.FR_SERVICE_TYPE"
)


def get_service_types_for_client(bq_client, client_id):
    query = f"""
        SELECT
            CAST(TYPE_ID AS INT64) AS TYPE_ID,
            DESCRIPTION,
            SAFE_CAST(RESERVICE AS INT64) AS API_RESERVICE,
            SAFE_CAST(REGULAR_SERVICE AS INT64) AS API_REGULAR_SERVICE,
            SAFE_CAST(FREQUENCY AS INT64) AS API_FREQUENCY,
            SAFE_CAST(DEFAULT_LENGTH AS INT64) AS API_DEFAULT_LENGTH,
            CLIENT AS clientId
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY CLIENT, TYPE_ID
                    ORDER BY PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', DATE_LOADED) DESC
                ) AS rn
            FROM `{SERVICE_TYPES_TABLE}`
            WHERE CLIENT = '{client_id}'
        )
        WHERE rn = 1
    """
    logger.info(f"Fetching service types for client: {client_id}")
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
    logger.info(f"Fetching merged service types for client: {client_id}")
    return bq_client.query(query).to_dataframe()
