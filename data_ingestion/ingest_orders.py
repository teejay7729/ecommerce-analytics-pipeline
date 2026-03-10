import pandas as pd
import os

RAW_PATH = "raw_data/"
PROCESSED_PATH = "processed_data/"

def ingest_orders():
    df = pd.read_csv(os.path.join(RAW_PATH, "olist_orders_dataset.csv"))
    df.to_parquet(os.path.join(PROCESSED_PATH, "orders_raw.parquet"), index=False)
    print("Orders ingested successfully.")

if __name__ == "__main__":
    ingest_orders()