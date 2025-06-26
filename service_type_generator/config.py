import os
from google.cloud import bigquery

# Optionally point to a service account key via environment variable
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# BigQuery table names can be overridden with environment variables
BQ_OUTPUT_TABLE = os.getenv("BQ_OUTPUT_TABLE", "kulti_test.full_service_type_logic")
ASK_CLIENT_TABLE = os.getenv("ASK_CLIENT_TABLE", "kulti_test.ask_client_flags")
SERVICE_TYPES_TABLE = os.getenv("SERVICE_TYPES_TABLE", "kulti_test.kulti_service_types")
MERGED_APPOINTMENT_TABLE = os.getenv("MERGED_APPOINTMENT_TABLE", "transformation_layer.merged_appointment")
MERGED_SUBSCRIPTION_TABLE = os.getenv("MERGED_SUBSCRIPTION_TABLE", "transformation_layer.merged_subscription")

BQ_OUTPUT_SCHEMA = [
    bigquery.SchemaField("TYPE_ID", "INT64"),
    bigquery.SchemaField("DESCRIPTION", "STRING"),
    bigquery.SchemaField("API RESERVICE FLAG", "INT64"),
    bigquery.SchemaField("API REGULAR_SERVICE FLAG", "INT64"),
    bigquery.SchemaField("API FREQUENCY FLAG", "INT64"),
    bigquery.SchemaField("API DEFAULT_LENGTH FLAG", "INT64"),
    bigquery.SchemaField("hasVisitsInPast2Years", "BOOL"),
    bigquery.SchemaField("hasActiveSubscription", "BOOL"),
    bigquery.SchemaField("Repeated Name", "BOOL"),
    bigquery.SchemaField("API Reservice", "BOOL"),
    bigquery.SchemaField("API Recurring", "BOOL"),
    bigquery.SchemaField("API Zero Time", "BOOL"),
    bigquery.SchemaField("API Has Reservice", "BOOL"),
    bigquery.SchemaField("Word Signal Reservice", "BOOL"),
    bigquery.SchemaField("Word Signal Recurring", "BOOL"),
    bigquery.SchemaField("Word Signal Zero Time", "BOOL"),
    bigquery.SchemaField("Word Signal Has Reservice", "BOOL"),
    bigquery.SchemaField("Expired Code", "BOOL"),
    bigquery.SchemaField("AskClient Reservice - Reason", "STRING"),
    bigquery.SchemaField("AskClient Recurring - Reason", "STRING"),
    bigquery.SchemaField("AskClient Zero Time - Reason", "STRING"),
    bigquery.SchemaField("AskClient Has Reservice - Reason", "STRING"),
    bigquery.SchemaField("AskClient", "BOOL"),
    bigquery.SchemaField("Client", "STRING"),
]
