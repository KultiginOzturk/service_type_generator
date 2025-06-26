from google.cloud import bigquery
import pandas as pd
from config import ASK_CLIENT_TABLE

def export_askclient_table(final_df):
    print("Exporting AskClient rows to BigQuery...")

    askclient_df = final_df[(final_df["AskClient"] == True) & (final_df["Expired Code"] == False)].copy()

    askclient_df["Recurrence"] = askclient_df["API FREQUENCY FLAG"]

    askclient_df["hasReservice"] = askclient_df.apply(
        lambda row: row["API Has Reservice"] or row["Word Signal Has Reservice"], axis=1
    )

    askclient_df["isRervice"] = askclient_df.apply(
        lambda row: row["API Reservice"] or row["Word Signal Reservice"], axis=1
    )

    askclient_df["zeroVisitTime"] = askclient_df.apply(
        lambda row: row["API Zero Time"] or row["Word Signal Zero Time"], axis=1
    )

    output_cols = [
        "TYPE_ID",
        "DESCRIPTION",
        "Recurrence",
        "hasReservice",
        "isRervice",
        "zeroVisitTime",
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
            bigquery.SchemaField("hasReservice", "BOOLEAN"),
            bigquery.SchemaField("isRervice", "BOOLEAN"),
            bigquery.SchemaField("zeroVisitTime", "BOOLEAN"),
            bigquery.SchemaField("clientId", "STRING"),
        ],
    )

    load_job = bq_client.load_table_from_dataframe(
        askclient_final,
        table_id,
        job_config=job_config
    )
    load_job.result()

    print(f"AskClient data uploaded to BigQuery table: {table_id}")
