import os
from google.cloud import bigquery

# Optionally point to a service account key via environment variable
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# Dataset IDs can be customized via environment variables
DATASET_ID = os.getenv("DATASET_ID", "kulti_test")
RAW_DATASET_ID = os.getenv("RAW_DATASET_ID", "raw_layer")
TRANSFORMATION_DATASET_ID = os.getenv("TRANSFORMATION_DATASET_ID", "transformation_layer")

# BigQuery table names can be overridden with environment variables. They default
# to the dataset IDs above combined with the table names used in development.
BQ_OUTPUT_TABLE = os.getenv("BQ_OUTPUT_TABLE", f"{DATASET_ID}.full_service_type_logic")
ASK_CLIENT_TABLE = os.getenv("ASK_CLIENT_TABLE", f"{DATASET_ID}.ask_client_flags")
SERVICE_TYPES_TABLE = os.getenv("SERVICE_TYPES_TABLE", f"{DATASET_ID}.kulti_service_types")
MERGED_APPOINTMENT_TABLE = os.getenv("MERGED_APPOINTMENT_TABLE", f"{TRANSFORMATION_DATASET_ID}.merged_appointment")
MERGED_SUBSCRIPTION_TABLE = os.getenv("MERGED_SUBSCRIPTION_TABLE", f"{TRANSFORMATION_DATASET_ID}.merged_subscription")
MERGED_SERVICE_TYPE_TABLE = os.getenv("MERGED_SERVICE_TYPE_TABLE", f"{TRANSFORMATION_DATASET_ID}.merged_service_type")

# Lookup table for service type recurrence mapping
LKP_RECURRING_TABLE = os.getenv(
    "LKP_RECURRING_TABLE",
    f"{RAW_DATASET_ID}.lkp_sales_mapping_recurring",
)

# Optional Google Drive folder ID for exporting per-client sheets
GOOGLE_SHEETS_FOLDER_ID = os.getenv("GOOGLE_SHEETS_FOLDER_ID")

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
