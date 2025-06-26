from config import MERGED_SUBSCRIPTION_TABLE


def get_subscriptions_for_client(bq_client, client_id):
    query = f"""
        SELECT
            subscriptionID,
            serviceID,
            serviceType,
            active,
            dateCancelled,
            clientID,
            dateAdded
        FROM `{MERGED_SUBSCRIPTION_TABLE}`
        WHERE clientID = '{client_id}'
    """
    print(f"Fetching subscriptions for client: {client_id}")
    return bq_client.query(query).to_dataframe()
