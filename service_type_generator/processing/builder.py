import pandas as pd

def build_final_dataframe(all_output_rows):
    print(f"Building final DataFrame from {len(all_output_rows)} rows...")
    return pd.DataFrame(all_output_rows)
