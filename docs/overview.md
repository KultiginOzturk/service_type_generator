# Service Type Generator — Overview

## Purpose

Determine final service-type flags (Recurring, Reservice, Zero-Time, Has Reservice) per client by combining API flags, curated sales mappings, keyword signals, and appointment/subscription usage. Produce AskClient recommendations where high-signal sources disagree or the service has high priority/revenue impact.

## Architecture

- Entry points
  - `main.py`: Full pipeline, filters out expired codes, exports to BigQuery, Excel, optional Google Sheets
  - `utils/unfiltered_main.py`: Same pipeline without Expired Code filter (Excel only)
- Layers
  - Data fetching: `data_fetching/*` queries BigQuery tables
  - Processing: `processing/analyzer.py` composes signals and applies constraints
  - Output: `output/*` uploads to BigQuery, writes Excel, Google Sheets
  - Utilities: `bq_client.py`, `utils/logger.py`, `utils/bq_catalog.py`

## Data Sources (BigQuery)

- Service types: `RAW_DATASET_ID.FR_SERVICE_TYPE` (overridable via `SERVICE_TYPES_TABLE`)
- Merged service types: `TRANSFORMATION_DATASET_ID.merged_service_type` (overridable)
- Appointments: `TRANSFORMATION_DATASET_ID.merged_appointment` (overridable)
- Subscriptions: `TRANSFORMATION_DATASET_ID.merged_subscription` (overridable)
- Recurring lookup: `RAW_DATASET_ID.lkp_sales_mapping_recurring` (overridable via `LKP_RECURRING_TABLE`)

## Configuration (env vars)

- Credentials and scopes
  - `GOOGLE_APPLICATION_CREDENTIALS`: service-account JSON; Drive + BigQuery scopes are used
- Datasets and tables
  - `DATASET_ID` (default: kulti_test)
  - `RAW_DATASET_ID` (default: raw_layer)
  - `TRANSFORMATION_DATASET_ID` (default: transformation_layer)
  - Optional overrides: `BQ_OUTPUT_TABLE`, `ASK_CLIENT_TABLE`, `SERVICE_TYPES_TABLE`, `MERGED_APPOINTMENT_TABLE`, `MERGED_SUBSCRIPTION_TABLE`, `MERGED_SERVICE_TYPE_TABLE`, `LKP_RECURRING_TABLE`
- Execution
  - `CLIENT_IDS`: comma-separated list (overridden by `--clients`)
  - `GOOGLE_SHEETS_FOLDER_ID`: enables per-client Google Sheets export

See `config.py` for keyword lists, API mapping rules, constraints, and output schema.

## Processing Logic (high level)

1) API signals: derive booleans from API flags per `API_SIGNAL_RULES`
2) Text signals: keyword matches yield True or None (never False)
3) Sales mapping: curated `isRecurring` values from lookup (True/False)
4) Appointments: recurring evidence via cadence bands and year-over-year checks; returns bool/None and score
5) Resolution: per metric priority, e.g. Recurring = SalesMapping > Appointments > API > Word
6) Business constraints: enforce consistency (e.g., recurring implies has_reservice)
7) AskClient: flag if top sources disagree or the service is high priority/revenue

## Outputs

- BigQuery
  - Full: `BQ_OUTPUT_TABLE` (schema in `config.py`)
  - AskClient subset: `ASK_CLIENT_TABLE`
- Excel
  - `final_df.xlsx` (All Data, AskClient True, AskClient False)
  - `askclient_final.xlsx` (subset)
- Google Sheets (optional)
  - One spreadsheet per client with the same three worksheets

## Runbook

1) Install dependencies: `pip install -r requirements.txt`
2) Set env vars (see Configuration)
3) Run
   - Filtered: `python main.py --clients ACCEL,CLIENT_B`
   - Unfiltered: `python utils/unfiltered_main.py --clients ACCEL`

## Operational Notes

- The Drive/Sheets operations require Drive and Spreadsheets scopes
- Write disposition for BQ loads is `WRITE_TRUNCATE`
- Appointment revenue share is optional; warnings are logged if columns are missing

## Glossary

- Recurring: Ongoing or scheduled services inferred from multiple signals
- Reservice: Follow-up or quality-control services
- Zero-Time: Services that should not count as a visit (e.g., billing-only)
- AskClient: Requires client review due to disagreement among high-priority sources or high impact


