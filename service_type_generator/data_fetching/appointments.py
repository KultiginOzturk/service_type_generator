import pandas as pd
from config import MERGED_APPOINTMENT_TABLE
from utils.logger import Logger

logger = Logger(__name__)

def get_appointments_for_client(bq_client, client_id):
    query = f"""
        SELECT
            individualAccountID,
            type,
            appointmentDate,
            clientID
        FROM `{MERGED_APPOINTMENT_TABLE}`
        WHERE clientID = '{client_id}'
    """
    logger.info(f"Fetching appointments for client: {client_id}")
    df = bq_client.query(query).to_dataframe()
    df['appointmentDate'] = pd.to_datetime(df['appointmentDate'], errors='coerce')
    return df
