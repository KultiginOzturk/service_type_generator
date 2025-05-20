from google.cloud import bigquery
import datetime
import pandas as pd
import os


def export_askclient_table(final_df):
    """
    Takes the DataFrame 'final_df' produced by your main logic
    (where each row has 'AskClient' among other columns),
    then filters for AskClient == True and outputs
    the requested columns to a new BQ table.
    """

    # 1) Filter for rows where AskClient == True and Expired Code = False
    askclient_df = final_df[(final_df["AskClient"] == True) & (final_df["Expired Code"] == False)].copy()

    # 2) Define new columns:
    askclient_df["Recurrence"] = askclient_df["API FREQUENCY FLAG"]

    askclient_df["hasReservice"] = askclient_df.apply(
        lambda row: row["API Has Reservice"] or row["Word Signal Has Reservice"],
        axis=1
    )

    askclient_df["isRervice"] = askclient_df.apply(
        lambda row: row["API Reservice"] or row["Word Signal Reservice"],
        axis=1
    )

    askclient_df["zeroVisitTime"] = askclient_df.apply(
        lambda row: row["API Zero Time"] or row["Word Signal Zero Time"],
        axis=1
    )

    # 3) Select only the columns we need in the final output
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

    # 4) Write to BigQuery
    bq_client = bigquery.Client()

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
    table_id = "kulti_test.ask_client_flags"
    askclient_final.rename(columns={"Client": "clientId"}, inplace = True)

    askclient_final.to_excel("askclient_final.xlsx", index = False)
    load_job = bq_client.load_table_from_dataframe(
        askclient_final,
        table_id,
        job_config=job_config
    )
    load_job.result()  # Wait for the job to complete

    print("AskClient rows successfully loaded into:",
          table_id)


def main():
    # Initialize BigQuery client
    os.environ[
        "GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Kültigin\PycharmProjects\pco_analytics\pco-qa-9a3d854dcb14.json"
    bq_client = bigquery.Client()

    # 1) Get the distinct list of clients from kulti_test.kulti_service_types
    distinct_clients_query = """
        SELECT DISTINCT CLIENT AS clientId
        FROM `kulti_test.kulti_service_types`
        WHERE CLIENT IS NOT NULL
    """
    print("Distinct clients selected")
    distinct_clients_df = bq_client.query(distinct_clients_query).to_dataframe()
    all_clients = distinct_clients_df['clientId'].tolist()

    # Prepare an empty list to collect all results for all clients
    master_rows = []

    # 2) Iterate through each client
    for client_id in all_clients:
        # --- Pull all relevant data for this one client ---
        print(f"Proccessing {client_id}")
        # kulti_service_types for this client
        service_types_query = f"""
            SELECT
                CAST(TYPE_ID as INT64) as TYPE_ID,
                DESCRIPTION,
                SAFE_CAST(RESERVICE AS INT64) as API_RESERVICE,
                SAFE_CAST(REGULAR_SERVICE AS INT64) as API_REGULAR_SERVICE,
                SAFE_CAST(FREQUENCY AS INT64) as API_FREQUENCY,
                SAFE_CAST(DEFAULT_LENGTH AS INT64) as API_DEFAULT_LENGTH,
                CLIENT as clientId
            FROM `kulti_test.kulti_service_types`
            WHERE CLIENT = '{client_id}'
        """
        service_types_df = bq_client.query(service_types_query).to_dataframe()

        print(f"Service Types queried for {client_id}")

        # merged_appointment for this client
        appointments_query = f"""
            SELECT
                individualAccountID,
                type,
                appointmentDate,
                clientID
            FROM `transformation_layer.merged_appointment`
            WHERE clientID = '{client_id}'
        """
        appointments_df = bq_client.query(appointments_query).to_dataframe()

        print(f"Appointments queried for {client_id}")

        appointments_df['appointmentDate'] = pd.to_datetime(appointments_df['appointmentDate'], errors='coerce')

        # merged_subscription for this client
        subscriptions_query = f"""
            SELECT
                subscriptionID,
                serviceID,
                serviceType,
                active,
                dateCancelled,
                clientID,
                dateAdded
            FROM `transformation_layer.merged_subscription`
            WHERE clientID = '{client_id}'
        """
        subs_df = bq_client.query(subscriptions_query).to_dataframe()

        print(f"Subscriptions queried for {client_id}")

        # 3) For each row in service_types_df, apply the logic
        output_rows = []
        now = pd.to_datetime("today")  # or we can pick a specific reference date

        for idx, row in service_types_df.iterrows():
            type_id = row["TYPE_ID"]
            desc = row["DESCRIPTION"] or ""

            api_reservice_flag = row["API_RESERVICE"] or 0
            api_regular_service_flag = row["API_REGULAR_SERVICE"] or 0
            api_frequency_flag = row["API_FREQUENCY"] or 0
            api_default_length_flag = row["API_DEFAULT_LENGTH"] or 0

            print(f"Computing API signals for {client_id}")

            # (A) Derive booleans from these INT flags
            api_reservice_bool = (api_reservice_flag == 1)
            api_recurring_bool = (api_regular_service_flag == 1) and (api_frequency_flag > 0)
            api_zero_time_bool = (api_default_length_flag == 0)

            # (B) Word signal checks
            desc_lower = desc.lower()

            print(f"Computing word signals for {client_id}")

            # Word Signal: Reservice
            word_reservice_bool = any(
                kw in desc_lower
                for kw in ["reservice", "call back", "callback", "qc", "quality control", "quality"]
            )

            # Word Signal: Recurring
            recurring_keywords = ["recurring", "monthly", "bi-weekly", "weekly"]
            word_recurring_bool = any(kw in desc_lower for kw in recurring_keywords)

            # Word Signal: Zero Time
            zero_time_keywords = ["equipment", "charge", "lead", "donation", "cancellation", "fee", "write off", "write-off"]
            word_zero_time_bool = any(kw in desc_lower for kw in zero_time_keywords)

            # Word Signal: HasReservice
            has_reservice_keywords = ["bed bug", "carpenter", "roach", "ant", "ants", "cockroach", "termite"]
            word_has_reservice_bool = any(kw in desc_lower for kw in has_reservice_keywords)

            # (C) hasVisitsInPast2Years

            print(f"Computing other fields for {client_id}")

            relevant_appts = appointments_df[
                (appointments_df['clientID'] == client_id) &
                (appointments_df['type'] == type_id)
            ]
            cutoff_date = now - pd.DateOffset(years=2)
            recent_appts = relevant_appts[relevant_appts['appointmentDate'] >= cutoff_date]
            has_visits_past_2yrs = (len(recent_appts) > 0)

            # (D) hasActiveSubscription
            relevant_subs = subs_df[subs_df['clientID'] == client_id]
            # match service references
            relevant_subs = relevant_subs[
                (relevant_subs['serviceID'] == type_id) |
                (relevant_subs['serviceID'] == str(type_id))
            ]
            active_subs = relevant_subs[
                (relevant_subs['active'] == 1) # &
                # (relevant_subs['dateCancelled'].isnull())
            ]
            has_active_subscription = (len(active_subs) > 0)

            # (E) Repeated Name
            repeated_name = False
            dup_count = (service_types_df['DESCRIPTION'] == desc).sum()
            if dup_count > 1:
                repeated_name = True

            # (F) API Has Reservice
            api_has_reservice_bool = api_reservice_bool or api_recurring_bool

            # (G) Expired Code: e.g. if no visits in 2 yrs OR no active sub
            expired_code_bool = (not has_visits_past_2yrs) and (not has_active_subscription)

            # (H) AskClient columns (reasons)
            askclient_reservice_reason = ""
            askclient_recurring_reason = ""
            askclient_zerotime_reason = ""
            askclient_hasreservice_reason = ""

            print(f"Comparing API and Word signals for {client_id}")

            # Compare API vs Word signals for each dimension:
            if api_reservice_bool != word_reservice_bool:
                askclient_reservice_reason = (
                    f"Conflict: API says reservice={api_reservice_bool}, word signals={word_reservice_bool}"
                )
            if api_recurring_bool != word_recurring_bool:
                askclient_recurring_reason = (
                    f"Conflict: API recurring={api_recurring_bool}, word recurring={word_recurring_bool}"
                )
            if api_zero_time_bool != word_zero_time_bool:
                askclient_zerotime_reason = (
                    f"Conflict: API zeroTime={api_zero_time_bool}, word zeroTime={word_zero_time_bool}"
                )
            if api_has_reservice_bool != word_has_reservice_bool:
                askclient_hasreservice_reason = (
                    f"Conflict: API hasReservice={api_has_reservice_bool}, word hasReservice={word_has_reservice_bool}"
                )

            # Overall AskClient
            askclient_bool = any([
                askclient_reservice_reason,
                askclient_recurring_reason,
                askclient_zerotime_reason,
                askclient_hasreservice_reason
            ])

            # Build final row
            final_row = {
                "TYPE_ID": type_id,
                "DESCRIPTION": desc,
                "API RESERVICE FLAG": api_reservice_flag,
                "API REGULAR_SERVICE FLAG": api_regular_service_flag,
                "API FREQUENCY FLAG": api_frequency_flag,
                "API DEFAULT_LENGTH FLAG": api_default_length_flag,
                "hasVisitsInPast2Years": has_visits_past_2yrs,
                "hasActiveSubscription": has_active_subscription,
                "Repeated Name": repeated_name,
                "API Reservice": api_reservice_bool,
                "API Recurring": api_recurring_bool,
                "API Zero Time": api_zero_time_bool,
                "API Has Reservice": api_has_reservice_bool,
                "Word Signal Reservice": word_reservice_bool,
                "Word Signal Recurring": word_recurring_bool,
                "Word Signal Zero Time": word_zero_time_bool,
                "Word Signal Has Reservice": word_has_reservice_bool,
                "Expired Code": expired_code_bool,
                "AskClient Reservice - Reason": askclient_reservice_reason,
                "AskClient Recurring - Reason": askclient_recurring_reason,
                "AskClient Zero Time - Reason": askclient_zerotime_reason,
                "AskClient Has Reservice - Reason": askclient_hasreservice_reason,
                "AskClient": askclient_bool,
                "Client": client_id
            }

            output_rows.append(final_row)

        # After processing all service types for this client, add to master list
        master_rows.extend(output_rows)

    # 4) Convert master_rows to a DataFrame
    final_df = pd.DataFrame(master_rows)

    # 5) Write to BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
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
    )

    export_askclient_table(final_df)

    final_df.to_excel("final_df.xlsx", index = False)
    print(f"Results saved to final_df.xlsx")
    table_id = "kulti_test.full_service_type_logic"
    load_job = bq_client.load_table_from_dataframe(
        final_df, table_id, job_config=job_config
    )
    load_job.result()  # Wait for the job to complete

    print("Data successfully loaded into kulti_test.full_service_type_logic")

if __name__ == "__main__":
    main()
