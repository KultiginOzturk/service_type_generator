from config import LKP_RECURRING_TABLE


def get_recurring_lookup_for_client(bq_client, client_id):
    query = f"""
        SELECT
            clientId,
            serviceType,
            isRecurring
        FROM `{LKP_RECURRING_TABLE}`
        WHERE clientId = '{client_id}'
    """
    print(f"Fetching recurring lookup for client: {client_id}")
    return bq_client.query(query).to_dataframe()
