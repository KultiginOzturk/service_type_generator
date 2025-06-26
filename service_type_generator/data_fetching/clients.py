from config import SERVICE_TYPES_TABLE


def get_distinct_clients(bq_client):
    query = f"""
        SELECT DISTINCT CLIENT AS clientId
        FROM `{SERVICE_TYPES_TABLE}`
        WHERE CLIENT IS NOT NULL
    """
    print("Fetching distinct clients...")
    df = bq_client.query(query).to_dataframe()
    return df['clientId'].tolist()
