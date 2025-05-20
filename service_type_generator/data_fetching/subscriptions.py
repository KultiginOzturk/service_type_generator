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
        FROM `transformation_layer.merged_subscription`
        WHERE clientID = '{client_id}'
    """
    print(f"Fetching subscriptions for client: {client_id}")
    return bq_client.query(query).to_dataframe()
