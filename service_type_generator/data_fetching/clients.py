def get_distinct_clients(bq_client):
    query = """
        SELECT DISTINCT CLIENT AS clientId
        FROM `kulti_test.kulti_service_types`
        WHERE CLIENT IS NOT NULL
    """
    print("Fetching distinct clients...")
    df = bq_client.query(query).to_dataframe()
    return df['clientId'].tolist()
