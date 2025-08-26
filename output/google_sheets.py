import os
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account
from utils.logger import Logger

logger = Logger(__name__)


def export_to_google_sheets(df, folder_id):
    """Export per-client data to Google Sheets with three worksheets."""
    logger.info("Exporting data to Google Sheets")

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not set")

    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=scopes
    )
    client = gspread.authorize(creds)

    existing_files = {
        f["name"]: f for f in client.list_spreadsheet_files(folder_id=folder_id)
    }

    def get_ws(sheet, name):
        try:
            return sheet.worksheet(name)
        except gspread.WorksheetNotFound:
            return sheet.add_worksheet(title=name, rows="100", cols="20")

    for client_id, client_df in df.groupby("Client"):
        title = str(client_id)
        if title in existing_files:
            sheet = client.open_by_key(existing_files[title]["id"])
        else:
            sheet = client.create(title, folder_id=folder_id)

        ws_all = get_ws(sheet, "All Data")
        ws_all.clear()
        set_with_dataframe(ws_all, client_df, include_index=False, resize=True)

        askclient_true = client_df[client_df["AskClient"] == True]
        ws_true = get_ws(sheet, "AskClient True")
        ws_true.clear()
        set_with_dataframe(ws_true, askclient_true, include_index=False, resize=True)

        askclient_false = client_df[client_df["AskClient"] == False].copy()
        # Mirror Excel exporter: use finalized signals
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

        ws_false = get_ws(sheet, "AskClient False")
        ws_false.clear()
        set_with_dataframe(ws_false, askclient_false, include_index=False, resize=True)

