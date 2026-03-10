import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(BASE_DIR, "raw_data")
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def ingest_payments():
    file_path = os.path.join(RAW_PATH, "olist_order_payments_dataset.csv")
    df = pd.read_csv(file_path)
    output_path = os.path.join(PROCESSED_PATH, "payments_raw.parquet")
    df.to_parquet(output_path, index=False)
    print("Payments ingested successfully.")

if __name__ == "__main__":
    ingest_payments()