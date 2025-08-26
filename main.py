from bq_client import get_bq_client
from data_fetching.clients import get_distinct_clients
from data_fetching.service_types import (
    get_service_types_for_client,
    get_merged_service_types_for_client,
)
from data_fetching.appointments import get_appointments_for_client
from data_fetching.subscriptions import get_subscriptions_for_client
from data_fetching.recurring_lookup import get_recurring_lookup_for_client
from processing.analyzer import analyze_service_type
from processing.builder import build_final_dataframe
from processing.filters import filter_active_subscription
from output.exporter import export_askclient_table, export_excel_with_sheets
from output.uploader import upload_to_bigquery
from output.google_sheets import export_to_google_sheets
from config import BQ_OUTPUT_TABLE, BQ_OUTPUT_SCHEMA, GOOGLE_SHEETS_FOLDER_ID
import argparse
import os
import pandas as pd
from utils.logger import Logger

logger = Logger(__name__)


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
        logger.info(f"Processing client: {client_id}")
        service_types_df = get_service_types_for_client(bq_client, client_id)
        merged_service_types_df = get_merged_service_types_for_client(bq_client, client_id)
        recurring_lookup_df = get_recurring_lookup_for_client(bq_client, client_id)
        logger.info(
            f"Rows fetched for {client_id} — service_types: {len(service_types_df)}, merged_service_types: {len(merged_service_types_df)}, recurring_lookup: {len(recurring_lookup_df)}"
        )

        # Merge lookup on description/serviceType to attach isRecurring info
        service_types_df = service_types_df.merge(
            recurring_lookup_df[["serviceType", "isRecurring"]],
            left_on="DESCRIPTION",
            right_on="serviceType",
            how="left",
        )
        if "serviceType" in service_types_df.columns:
            service_types_df.drop(columns=["serviceType"], inplace=True)

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
            logger.warning(
                f"merged_service_type mismatches for client {client_id}"
            )
            logger.warning(
                mismatched[["TYPE_ID", "DESCRIPTION", "DESCRIPTION_MERGED"]].to_dict(orient="records")
            )
        appointments_df = get_appointments_for_client(bq_client, client_id)
        subscriptions_df = get_subscriptions_for_client(bq_client, client_id)
        logger.info(
            f"Rows fetched for {client_id} — appointments: {len(appointments_df)}, subscriptions: {len(subscriptions_df)}"
        )

        # Compute appointment share per service type for prioritization
        appt_share_pct_by_type = {}
        top20_type_ids = set()
        try:
            if not appointments_df.empty:
                ap = appointments_df.copy()
                ap['type_int'] = pd.to_numeric(ap['type'], errors='coerce')
                counts = ap.dropna(subset=['type_int']).groupby('type_int').size().reset_index(name='appointmentCount')
                total = counts['appointmentCount'].sum()
                if total and total > 0:
                    counts['appointmentSharePct'] = (counts['appointmentCount'] / total * 100).round(2)
                    appt_share_pct_by_type = {int(row.type_int): float(row.appointmentSharePct) for _, row in counts.iterrows()}
                    top20 = counts.sort_values('appointmentCount', ascending=False).head(20)
                    top20_type_ids = set(top20['type_int'].astype(int).tolist())
        except Exception as e:
            logger.warning(f"Failed computing appointment share for client {client_id}: {e}")

        for _, row in service_types_df.iterrows():
            result_row = analyze_service_type(
                row, appointments_df, subscriptions_df, service_types_df, now, client_id,
                appt_share_pct_by_type=appt_share_pct_by_type, top20_type_ids=top20_type_ids
            )
            all_rows.append(result_row)

    final_df = build_final_dataframe(all_rows)
    # Filter to rows with an active subscription before exporting
    final_df = filter_active_subscription(final_df)

    export_askclient_table(final_df)
    upload_to_bigquery(final_df, BQ_OUTPUT_TABLE, BQ_OUTPUT_SCHEMA)
    if GOOGLE_SHEETS_FOLDER_ID:
        export_to_google_sheets(final_df, GOOGLE_SHEETS_FOLDER_ID)
    export_excel_with_sheets(final_df, "final_df.xlsx")
    logger.info("Done.")


if __name__ == "__main__":
    main()
