import pandas as pd

def get_appointments_for_client(bq_client, client_id):
    query = f"""
        SELECT
            individualAccountID,
            type,
            appointmentDate,
            clientID
        FROM `transformation_layer.merged_appointment`
        WHERE clientID = '{client_id}'
    """
    print(f"Fetching appointments for client: {client_id}")
    df = bq_client.query(query).to_dataframe()
    df['appointmentDate'] = pd.to_datetime(df['appointmentDate'], errors='coerce')
    return df
