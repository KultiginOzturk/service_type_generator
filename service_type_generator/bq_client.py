from google.cloud import bigquery

def get_bq_client():
    return bigquery.Client()
