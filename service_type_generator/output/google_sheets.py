import os
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account


def export_to_google_sheets(df, sheet_id, worksheet="Sheet1"):
    """Export a DataFrame to a Google Sheet using a service account."""
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not set")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=scopes
    )
    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id)
    try:
        ws = sheet.worksheet(worksheet)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=worksheet, rows="100", cols="20")

    ws.clear()
    set_with_dataframe(ws, df, include_index=False, resize=True)

