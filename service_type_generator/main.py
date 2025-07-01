from bq_client import get_bq_client
from data_fetching.clients import get_distinct_clients
from data_fetching.service_types import (
    get_service_types_for_client,
    get_merged_service_types_for_client,
)
from data_fetching.appointments import get_appointments_for_client
from data_fetching.subscriptions import get_subscriptions_for_client
from processing.analyzer import analyze_service_type
from processing.builder import build_final_dataframe
from processing.filters import filter_active_subscription
from output.exporter import export_askclient_table
from output.uploader import upload_to_bigquery
from output.google_sheets import export_to_google_sheets
from config import BQ_OUTPUT_TABLE, BQ_OUTPUT_SCHEMA, GOOGLE_SHEET_ID
import argparse
import os
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--clients",
        help="Comma-separated list of client IDs to process. Overrides CLIENT_IDS env var",
    )
    args = parser.parse_args()

    bq_client = get_bq_client()

    if args.clients:
        clients = [c.strip() for c in args.clients.split(",") if c.strip()]
    else:
        env_clients = os.getenv("CLIENT_IDS")
        if env_clients:
            clients = [c.strip() for c in env_clients.split(",") if c.strip()]
        else:
            clients = get_distinct_clients(bq_client)
    all_rows = []
    now = pd.to_datetime("today")

    for client_id in clients:
        print(f"Processing client: {client_id}")
        service_types_df = get_service_types_for_client(bq_client, client_id)
        merged_service_types_df = get_merged_service_types_for_client(bq_client, client_id)

        merged_check = service_types_df.merge(
            merged_service_types_df[["TYPE_ID", "DESCRIPTION"]],
            on="TYPE_ID",
            how="left",
            suffixes=("", "_MERGED"),
        )
        mismatched = merged_check[
            merged_check["DESCRIPTION_MERGED"].isna()
            | (merged_check["DESCRIPTION_MERGED"] != merged_check["DESCRIPTION"])
        ]
        if not mismatched.empty:
            print(
                f"Warning: merged_service_type mismatches for client {client_id}"
            )
            print(mismatched[["TYPE_ID", "DESCRIPTION", "DESCRIPTION_MERGED"]])
        appointments_df = get_appointments_for_client(bq_client, client_id)
        subscriptions_df = get_subscriptions_for_client(bq_client, client_id)

        for _, row in service_types_df.iterrows():
            result_row = analyze_service_type(
                row, appointments_df, subscriptions_df, service_types_df, now, client_id
            )
            all_rows.append(result_row)

    final_df = build_final_dataframe(all_rows)
    # Filter to rows with an active subscription before exporting
    final_df = filter_active_subscription(final_df)

    export_askclient_table(final_df)
    upload_to_bigquery(final_df, BQ_OUTPUT_TABLE, BQ_OUTPUT_SCHEMA)
    if GOOGLE_SHEET_ID:
        export_to_google_sheets(final_df, GOOGLE_SHEET_ID)
    final_df.to_excel("final_df.xlsx", index=False)
    print("Done.")


if __name__ == "__main__":
    main()
