from bq_client import get_bq_client
from data_fetching.clients import get_distinct_clients
from data_fetching.service_types import get_service_types_for_client
from data_fetching.appointments import get_appointments_for_client
from data_fetching.subscriptions import get_subscriptions_for_client
from processing.analyzer import analyze_service_type
from processing.builder import build_final_dataframe
from output.exporter import export_askclient_table
from output.uploader import upload_to_bigquery
from config import BQ_OUTPUT_TABLE, BQ_OUTPUT_SCHEMA
import pandas as pd


def main():
    bq_client = get_bq_client()
    clients = get_distinct_clients(bq_client)
    all_rows = []
    now = pd.to_datetime("today")

    for client_id in clients:
        print(f"Processing client: {client_id}")
        service_types_df = get_service_types_for_client(bq_client, client_id)
        appointments_df = get_appointments_for_client(bq_client, client_id)
        subscriptions_df = get_subscriptions_for_client(bq_client, client_id)

        for _, row in service_types_df.iterrows():
            result_row = analyze_service_type(
                row, appointments_df, subscriptions_df, service_types_df, now, client_id
            )
            all_rows.append(result_row)

    final_df = build_final_dataframe(all_rows)
    export_askclient_table(final_df)
    upload_to_bigquery(final_df, BQ_OUTPUT_TABLE, BQ_OUTPUT_SCHEMA)
    final_df.to_excel("final_df.xlsx", index=False)
    print("Done.")


if __name__ == "__main__":
    main()
