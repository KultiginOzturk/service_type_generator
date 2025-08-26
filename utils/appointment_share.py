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


def build_share_for_client(bq_client, client_id: str) -> pd.DataFrame:
    logger.info(f"Processing appointment share for client: {client_id}")
    service_types_df = get_service_types_for_client(bq_client, client_id)
    appointments_df = get_appointments_for_client(bq_client, client_id)

    share_df = compute_appointment_share(appointments_df, service_types_df)
    if share_df.empty:
        logger.warning(f"No appointment data for client {client_id}; skipping.")
        return pd.DataFrame(columns=["clientId", "TYPE_ID", "service", "appointmentCount", "appointmentSharePct"])  # empty

    share_df.insert(0, "clientId", client_id)
    return share_df


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

    all_results = []
    for client_id in clients:
        try:
            df = build_share_for_client(bq_client, client_id)
            if df is not None and not df.empty:
                all_results.append(df)
        except Exception as exc:
            logger.error(f"Failed to compute appointment share for {client_id}: {exc}")

    if not all_results:
        logger.warning("No appointment share data to write.")
        return

    final_df = pd.concat(all_results, ignore_index=True)
    final_df.sort_values(by=["clientId", "appointmentCount"], ascending=[True, False], inplace=True)
    output_path = "appointment_share.xlsx"
    final_df.to_excel(output_path, index=False)
    logger.info(f"Appointment share written: {output_path}")


if __name__ == "__main__":
    main()


