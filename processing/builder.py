"""
Processing — Builder

Thin wrapper to assemble the final pandas DataFrame from per-service-type
analysis rows.
"""
import pandas as pd
from utils.logger import Logger

logger = Logger(__name__)

def build_final_dataframe(all_output_rows):
    logger.info(f"Building final DataFrame from {len(all_output_rows)} rows...")
    return pd.DataFrame(all_output_rows)
