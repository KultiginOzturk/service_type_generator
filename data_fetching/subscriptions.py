from config import MERGED_SUBSCRIPTION_TABLE
from utils.logger import Logger

logger = Logger(__name__)


def get_subscriptions_for_client(bq_client, client_id):
    query = f"""
        SELECT
            subscriptionID,
            serviceID,
            serviceType,
            annualRecurringServices,
            active,
            dateCancelled,
            clientID,
            dateAdded
        FROM `{MERGED_SUBSCRIPTION_TABLE}`
        WHERE clientID = '{client_id}'
    """
    logger.info(f"Fetching subscriptions for client: {client_id}")
    return bq_client.query(query).to_dataframe()
