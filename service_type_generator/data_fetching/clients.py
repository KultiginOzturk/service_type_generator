import os

# Table ID can be overridden with environment variables. It defaults to the
# DATASET_ID defined in config combined with the service types table name.
from config import DATASET_ID
from utils.logger import Logger

logger = Logger(__name__)

SERVICE_TYPES_TABLE = os.getenv(
    "SERVICE_TYPES_TABLE", f"{DATASET_ID}.kulti_service_types"
)


def get_distinct_clients(bq_client):
    query = f"""
        SELECT DISTINCT CLIENT AS clientId
        FROM `{SERVICE_TYPES_TABLE}`
        WHERE CLIENT IS NOT NULL
    """
    logger.info("Fetching distinct clients...")
    df = bq_client.query(query).to_dataframe()
    return df['clientId'].tolist()
