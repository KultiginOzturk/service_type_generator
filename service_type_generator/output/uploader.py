from google.cloud import bigquery

def upload_to_bigquery(df, table_id, schema):
    print(f"Uploading full results to {table_id}...")

    bq_client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema
    )

    load_job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()

    print(f"Upload complete: {table_id}")
