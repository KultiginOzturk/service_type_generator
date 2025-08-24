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
SERVICE_TYPES_TABLE = os.getenv(
    "SERVICE_TYPES_TABLE", f"{RAW_DATASET_ID}.FR_SERVICE_TYPE"
)
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

# Keyword lists for text-based signal detection
WORD_SIGNALS = {
    "reservice": [
        "reservice", "call back", "callback", "qc", "quality control", "quality"
    ],
    "recurring": [
        "seasonal","recurring", "monthly", "bi-weekly", "weekly", "quarterly", "yearly", "yard", "yard service", "annual", "annually", "bimonthly", "biweekly", "weekly", "per month", "per week", "per quarter", "per year"
    ],
    "zero_time": [
        "equipment", "charge", "lead", "donation", "cancellation", "fee", "write off", "write-off", "bait", "payment", "collection", "return"
    ],
    "has_reservice": [
        "bed bug", "carpenter", "roach", "ant", "ants", "cockroach", "termite", "pest", "home", "yard"
    ]
}

# Business rules for API signal mapping
API_SIGNAL_RULES = {
    "FREQUENCY": {
        # isRecurring: if > 0 then True
        "isRecurring": lambda x: True if x is not None and x > 0 else None,
        "has_reservice": None,
        # isRervice: if = 0 then True
        # "isRervice": lambda x: True if x == 0 else None,
        "isRervice": None,
        "zeroVisitTime": None,
    },
    "RESERVICE": {
        # isRecurring: if = 1 then False
        "isRecurring": lambda x: False if x == 1 else None,
        # has_reservice: if = 1 then False
        "has_reservice": lambda x: False if x == 1 else None,
        # isRervice: if = 1 then True
        "isRervice": lambda x: True if x == 1 else False,
        "zeroVisitTime": None,
    },
    "DEFAULT_LENGTH": {
        "isRecurring": None,
        "has_reservice": None,
        "isRervice": None,
        # zeroVisitTime: if = 0 then True
        "zeroVisitTime": lambda x: True if x == 0 else None,
    },
    "INITIAL_ID": {
        # isRecurring: rule removed
        "isRecurring": None,
        "has_reservice": None,
        "zeroVisitTime": None,
    },
    "REGULAR_SERVICE": {
        # All REGULAR_SERVICE rules removed
        "isRecurring": None,
        "has_reservice": None,
        "isRervice": None,
        "zeroVisitTime": None,
    },
    "INITIAL": {
        # isRecurring: if = 1 then False
        "isRecurring": lambda x: False if x == 1 else None,
        "has_reservice": None,
        # isRervice: if = 1 then False
        "isRervice": lambda x: False if x == 1 else None,
        # zeroVisitTime: if = 1 then False
        "zeroVisitTime": lambda x: False if x == 1 else None,
    },
}

# Business logic constraints
BUSINESS_CONSTRAINTS = {
    "isRecurring_isRervice": "isRecurring=True cannot have isRervice=True",
    "isRervice_hasReservice": "isRervice=True cannot have has_reservice=True",
    "zeroVisitTime_has_reservice": "zeroVisitTime=True cannot have has_reservice=True",
    # Informational rule (not a violation): recurring=True forces has_reservice=True
    "recurring_implies_has_reservice": "recurring=True forces has_reservice=True"
}

BQ_OUTPUT_SCHEMA = [
    bigquery.SchemaField("TYPE_ID", "INT64"),
    bigquery.SchemaField("DESCRIPTION", "STRING"),
    bigquery.SchemaField("API RESERVICE FLAG", "INT64"),
    bigquery.SchemaField("API REGULAR_SERVICE FLAG", "INT64"),
    bigquery.SchemaField("API FREQUENCY FLAG", "INT64"),
    bigquery.SchemaField("API DEFAULT_LENGTH FLAG", "INT64"),
    bigquery.SchemaField("API INITIAL ID FLAG", "INT64"),
    bigquery.SchemaField("API INITIAL FLAG", "INT64"),
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
    bigquery.SchemaField("Final Reservice", "BOOL"),
    bigquery.SchemaField("Final Recurring", "BOOL"),
    bigquery.SchemaField("Final Zero Time", "BOOL"),
    bigquery.SchemaField("Final Has Reservice", "BOOL"),
    bigquery.SchemaField("Expired Code", "BOOL"),
    bigquery.SchemaField("AskClient Reservice - Reason", "STRING"),
    bigquery.SchemaField("AskClient Recurring - Reason", "STRING"),
    bigquery.SchemaField("AskClient Zero Time - Reason", "STRING"),
    bigquery.SchemaField("AskClient Has Reservice - Reason", "STRING"),
    bigquery.SchemaField("AskClient", "BOOL"),
    bigquery.SchemaField("Client", "STRING"),
]
