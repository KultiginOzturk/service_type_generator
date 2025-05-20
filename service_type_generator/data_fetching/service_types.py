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
        FROM `kulti_test.kulti_service_types`
        WHERE CLIENT = '{client_id}'
    """
    print(f"Fetching service types for client: {client_id}")
    return bq_client.query(query).to_dataframe()
