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

        # De-duplicate service types per client by TYPE_ID before analysis
        before_dedup = len(service_types_df)
        service_types_df = service_types_df.drop_duplicates(subset=["TYPE_ID"])  # safe no-op if already unique
        after_dedup = len(service_types_df)
        if after_dedup < before_dedup:
            logger.info(f"De-duplicated service types for {client_id}: {before_dedup} -> {after_dedup}")

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

        # Compute revenue share per service type from subscriptions (annualRecurringServices)
        revenue_share_pct_by_type = {}
        top10_revenue_type_ids = set()
        try:
            subs = subscriptions_df.copy()
            if not subs.empty:
                # Active subscriptions only
                subs_active = subs[(subs['active'] == True) & (subs['dateCancelled'].isnull())]
                # Normalize serviceID type and annualRecurringServices numeric
                subs_active['service_int'] = pd.to_numeric(subs_active['serviceID'], errors='coerce')
                ars = subs_active.get('annualRecurringServices')
                if ars is None:
                    logger.warning(f"annualRecurringServices column not found in subscriptions for {client_id}. Available columns: {list(subs_active.columns)}")
                else:
                    ars_str = ars.astype(str).str.strip()
                    ars_str = ars_str.str.replace(r'[,$]', '', regex=True)
                    neg_mask = ars_str.str.match(r'^\(.*\)$', na=False)
                    ars_str = ars_str.str.replace(r'[()]', '', regex=True)
                    subs_active['ars_num'] = pd.to_numeric(ars_str, errors='coerce')
                    subs_active.loc[neg_mask, 'ars_num'] = -subs_active.loc[neg_mask, 'ars_num'].abs()
                    subs_active = subs_active.dropna(subset=['service_int', 'ars_num'])
                    if not subs_active.empty:
                        sums = subs_active.groupby('service_int')['ars_num'].sum().reset_index(name='ars_total')
                        total_ars = float(sums['ars_total'].sum())
                        logger.info(f"Total annualRecurringServices for {client_id}: {total_ars:.2f} across {len(sums)} service types")
                        if total_ars and total_ars > 0:
                            sums['revenueSharePct'] = (sums['ars_total'] / total_ars * 100).round(2)
                            revenue_share_pct_by_type = {int(row.service_int): float(row.revenueSharePct) for _, row in sums.iterrows()}
                            top10 = sums.sort_values('ars_total', ascending=False).head(10)
                            top10_revenue_type_ids = set(top10['service_int'].astype(int).tolist())
        except Exception as e:
            logger.warning(f"Failed computing revenue share (subscriptions) for client {client_id}: {e}")

        client_rows = []
        for _, row in service_types_df.iterrows():
            result_row = analyze_service_type(
                row, appointments_df, subscriptions_df, service_types_df, now, client_id,
                appt_share_pct_by_type=appt_share_pct_by_type, top20_type_ids=top20_type_ids,
                revenue_share_pct_by_type=revenue_share_pct_by_type, top10_revenue_type_ids=top10_revenue_type_ids
            )
            all_rows.append(result_row)
            client_rows.append(result_row)

        # After finishing this client, export its results to Google Sheets to avoid rate limits later
        client_final_df = build_final_dataframe(client_rows)
        client_final_df = filter_active_subscription(client_final_df)
        if GOOGLE_SHEETS_FOLDER_ID:
            try:
                export_to_google_sheets(client_final_df, GOOGLE_SHEETS_FOLDER_ID)
            except Exception as e:
                logger.warning(f"Google Sheets export failed for client {client_id}: {e}")

    final_df = build_final_dataframe(all_rows)
    # Filter to rows with an active subscription before exporting
    final_df = filter_active_subscription(final_df)

    export_askclient_table(final_df)
    upload_to_bigquery(final_df, BQ_OUTPUT_TABLE, BQ_OUTPUT_SCHEMA)
    # Skip Google Sheets bulk export; already exported per-client above to reduce rate limits
    export_excel_with_sheets(final_df, "final_df.xlsx")
    logger.info("Done.")


if __name__ == "__main__":
    main()
