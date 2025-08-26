# Service Type Signal Analyzer

This project analyzes service type data by comparing API flags with keyword-based signals in service descriptions. It identifies mismatches and flags them for client review.

## Features

- Fetches service type, appointment, and subscription data from BigQuery
- Validates service type rows against the `merged_service_type` table
- Applies logic to compare API flags with keyword-based text signals
- Flags services with conflicting data (`AskClient`)
- Outputs full data and filtered results to BigQuery
- Exports Excel files for review

## Project Structure

your_project/
│
├── main.py                     # Entry point
├── config.py                   # Config settings like credentials, table names
├── bq_client.py                # Google BigQuery client setup
│
├── data_fetching/
│   ├── __init__.py
│   ├── clients.py              # get_distinct_clients
│   ├── service_types.py        # get_service_types_for_client
│   ├── appointments.py         # get_appointments_for_client
│   └── subscriptions.py        # get_subscriptions_for_client
│
├── processing/
│   ├── __init__.py
│   ├── analyzer.py             # analyze_service_type
│   └── builder.py              # build_final_dataframe
│
├── output/
│   ├── __init__.py
│   ├── exporter.py             # export_askclient_table
│   └── uploader.py             # upload_to_bigquery
│
└── requirements.txt            # Package dependencies



## How to Use

### 1. Install Dependencies

```bash
pip install -r requirements.txt

### 2. Configure Environment
Set the following environment variables before running the script:

- `GOOGLE_APPLICATION_CREDENTIALS` - path to your service account key
- `GOOGLE_SHEETS_FOLDER_ID` - optional Drive folder for exporting per-client Google Sheets
- `DATASET_ID` - dataset containing service and output tables (default: `kulti_test`)
- `TRANSFORMATION_DATASET_ID` - dataset for appointment/subscription tables (default: `transformation_layer`)
- `BQ_OUTPUT_TABLE` - full results table (defaults to `DATASET_ID.full_service_type_logic`)
- `ASK_CLIENT_TABLE` - AskClient subset table (defaults to `DATASET_ID.ask_client_flags`)
- `CLIENT_IDS` - optional comma-separated list of client IDs to process. Overrides automatic lookup.

### 3. Run the Script

python main.py [--clients id1,id2]

Outputs
Full logic results to: value of `BQ_OUTPUT_TABLE`

Filtered AskClient results to: value of `ASK_CLIENT_TABLE`

Excel exports: final_df.xlsx, askclient_final.xlsx
Per-client Google Sheets: created or updated in the folder set by `GOOGLE_SHEETS_FOLDER_ID`

Notes
String matching is used for detecting word signals.
