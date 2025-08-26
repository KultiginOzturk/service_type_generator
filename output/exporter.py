from google.cloud import bigquery
import pandas as pd
import os
from config import DATASET_ID
from utils.logger import Logger

logger = Logger(__name__)

ASK_CLIENT_TABLE = os.getenv(
    "ASK_CLIENT_TABLE", f"{DATASET_ID}.ask_client_flags"
)

def export_askclient_table(final_df):
    logger.info("Exporting AskClient rows to BigQuery...")

    askclient_df = final_df[(final_df["AskClient"] == True) & (final_df["Expired Code"] == False)].copy()

    askclient_df["Recurrence"] = askclient_df["API FREQUENCY FLAG"]

    # Use the finalized signals that were resolved in the analyzer
    askclient_df["hasReservice"] = askclient_df["Final Has Reservice"]
    askclient_df["isRervice"] = askclient_df["Final Reservice"]
    askclient_df["zeroVisitTime"] = askclient_df["Final Zero Time"]

    output_cols = [
        "TYPE_ID",
        "DESCRIPTION",
        "Recurrence",
        "hasReservice",
        "isRervice",
        "zeroVisitTime",
        "Appointment Share Pct",
        "Client"
    ]

    askclient_final = askclient_df[output_cols].copy()
    askclient_final.rename(columns={"Client": "clientId"}, inplace=True)

    # Save to Excel
    askclient_final.to_excel("askclient_final.xlsx", index=False)

    # Upload to BQ
    bq_client = bigquery.Client()
    table_id = ASK_CLIENT_TABLE

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("TYPE_ID", "INT64"),
            bigquery.SchemaField("DESCRIPTION", "STRING"),
            bigquery.SchemaField("Recurrence", "INT64"),
            bigquery.SchemaField("hasReservice", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("isRervice", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("zeroVisitTime", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("Appointment Share Pct", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("clientId", "STRING"),
        ],
    )

    load_job = bq_client.load_table_from_dataframe(
        askclient_final,
        table_id,
        job_config=job_config
    )
    load_job.result()

    logger.info(f"AskClient data uploaded to BigQuery table: {table_id}")


def export_excel_with_sheets(final_df, filename="final_df.xlsx"):
    """Create an Excel workbook with 3 sheets as specified."""
    logger.info(f"Writing Excel report to {filename}...")

    askclient_true = final_df[final_df["AskClient"] == True].copy()

    askclient_false = final_df[final_df["AskClient"] == False].copy()
    # Use the finalized signals that were resolved in the analyzer
    askclient_false["Reservice"] = askclient_false["Final Reservice"]
    askclient_false["Recurring"] = askclient_false["Final Recurring"]
    askclient_false["Zero Time"] = askclient_false["Final Zero Time"]
    askclient_false["Has Reservice"] = askclient_false["Final Has Reservice"]
    summary_cols = [
        "TYPE_ID",
        "DESCRIPTION",
        "Reservice",
        "Recurring",
        "Zero Time",
        "Has Reservice",
        "API FREQUENCY FLAG",
        "API RESERVICE FLAG",
        "API REGULAR_SERVICE FLAG",
        "API DEFAULT_LENGTH FLAG",
        "hasVisitsInPast2Years",
        "hasActiveSubscription",
        "Expired Code",
        "Client",
    ]
    askclient_false = askclient_false[summary_cols]

    with pd.ExcelWriter(filename) as writer:
        final_df.to_excel(writer, index=False, sheet_name="All Data")
        askclient_true.to_excel(writer, index=False, sheet_name="AskClient True")
        askclient_false.to_excel(writer, index=False, sheet_name="AskClient False")

    logger.info("Excel report written")
