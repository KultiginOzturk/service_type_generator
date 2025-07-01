import pandas as pd

def filter_active_subscription(df: pd.DataFrame) -> pd.DataFrame:
    """Return only rows with an active subscription."""
    return df[df["hasActiveSubscription"]]
