import argparse
import os
import pandas as pd

from bq_client import get_bq_client
from data_fetching.clients import get_distinct_clients
from data_fetching.service_types import get_service_types_for_client
from data_fetching.appointments import get_appointments_for_client
from utils.logger import Logger


logger = Logger(__name__)


def compute_appointment_share(appointments_df: pd.DataFrame, service_types_df: pd.DataFrame) -> pd.DataFrame:
    """Return appointment counts and percentage share by service.

    The function expects appointments to contain a column named `type` that
    corresponds to `TYPE_ID` in service types. It will map to `DESCRIPTION`.
    """
    if appointments_df is None or appointments_df.empty:
        return pd.DataFrame(columns=["TYPE_ID", "DESCRIPTION", "appointmentCount", "appointmentSharePct"])  # empty

    appt_df = appointments_df.copy()
    # Normalize column for join
    if "TYPE_ID" not in appt_df.columns and "type" in appt_df.columns:
        appt_df.rename(columns={"type": "TYPE_ID"}, inplace=True)

    if "TYPE_ID" not in appt_df.columns:
        # No type identifier present; count total only
        appt_df["TYPE_ID"] = None

    appt_df["TYPE_ID"] = pd.to_numeric(appt_df["TYPE_ID"], errors="coerce")

    counts = appt_df.groupby("TYPE_ID", dropna=False).size().reset_index(name="appointmentCount")

    # Map descriptions when available
    service_map = (
        service_types_df[["TYPE_ID", "DESCRIPTION"]].copy()
        if service_types_df is not None and not service_types_df.empty
        else pd.DataFrame(columns=["TYPE_ID", "DESCRIPTION"])
    )

    result = counts.merge(service_map, on="TYPE_ID", how="left")

    # Compute share
    total_appointments = result["appointmentCount"].sum()
    if total_appointments and total_appointments > 0:
        result["appointmentSharePct"] = (result["appointmentCount"] / total_appointments * 100).round(2)
    else:
        result["appointmentSharePct"] = 0.0

    # Finalize columns and ordering
    result["service"] = result["DESCRIPTION"].fillna(result["TYPE_ID"].astype(str))
    result = result[["TYPE_ID", "service", "appointmentCount", "appointmentSharePct"]]
    result.sort_values(by="appointmentCount", ascending=False, inplace=True)
    result.reset_index(drop=True, inplace=True)
    return result


def export_appointment_share_for_client(bq_client, client_id: str) -> None:
    logger.info(f"Processing appointment share for client: {client_id}")
    service_types_df = get_service_types_for_client(bq_client, client_id)
    appointments_df = get_appointments_for_client(bq_client, client_id)

    share_df = compute_appointment_share(appointments_df, service_types_df)
    if share_df.empty:
        logger.warning(f"No appointment data for client {client_id}; skipping export.")
        return

    share_df.insert(0, "clientId", client_id)
    output_path = f"appointment_share_{client_id}.xlsx"
    share_df.to_excel(output_path, index=False)
    logger.info(f"Appointment share written: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Export appointment share by service")
    parser.add_argument(
        "--clients",
        help="Comma-separated list of client IDs to process. Overrides CLIENT_IDS env var",
    )
    args = parser.parse_args()

    bq_client = get_bq_client()

    if args.clients:
        clients = [c.strip() for c in args.clients.split(",") if c.strip()]
    else:
        env_clients = os.getenv("CLIENT_IDS")
        if env_clients:
            clients = [c.strip() for c in env_clients.split(",") if c.strip()]
        else:
            clients = get_distinct_clients(bq_client)

    for client_id in clients:
        try:
            export_appointment_share_for_client(bq_client, client_id)
        except Exception as exc:
            logger.error(f"Failed to export appointment share for {client_id}: {exc}")


if __name__ == "__main__":
    main()


