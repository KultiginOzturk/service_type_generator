import pandas as pd

def filter_active_subscription(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows with an active subscription OR visits in the past 2 years."""
    if "hasActiveSubscription" not in df.columns or "hasVisitsInPast2Years" not in df.columns:
        return df
    return df[(df["hasActiveSubscription"]) | (df["hasVisitsInPast2Years"])]
