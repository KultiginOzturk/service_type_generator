import os
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2 import service_account


def export_to_google_sheets(df, folder_id, worksheet="Sheet1"):
    """Export each client's data to a sheet inside the given Drive folder.

    If a spreadsheet already exists for a client (named after the client ID)
    it will be updated. Otherwise a new spreadsheet will be created in the
    specified folder.
    """
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

    # Cache existing files in the folder to avoid extra API calls
    existing_files = {
        f["name"]: f
        for f in client.list_spreadsheet_files(parent_id=folder_id)
    }

    for client_id, client_df in df.groupby("Client"):
        title = str(client_id)
        if title in existing_files:
            sheet = client.open_by_key(existing_files[title]["id"])
        else:
            sheet = client.create(title, folder_id=folder_id)

        try:
            ws = sheet.worksheet(worksheet)
        except gspread.WorksheetNotFound:
            ws = sheet.add_worksheet(title=worksheet, rows="100", cols="20")

        ws.clear()
        set_with_dataframe(ws, client_df, include_index=False, resize=True)

