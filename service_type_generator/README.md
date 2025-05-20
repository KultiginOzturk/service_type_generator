# Service Type Signal Analyzer

This project analyzes service type data by comparing API flags with keyword-based signals in service descriptions. It identifies mismatches and flags them for client review.

## Features

- Fetches service type, appointment, and subscription data from BigQuery
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

### 2. Configure Credentials
Set the path to your Google Cloud service account key in config.py. Do not commit the credential file to version control.


### 3. Run the Script

python main.py

Outputs
Full logic results to: kulti_test.full_service_type_logic

Filtered AskClient results to: kulti_test.ask_client_flags

Excel exports: final_df.xlsx, askclient_final.xlsx

Notes
String matching is used for detecting word signals.