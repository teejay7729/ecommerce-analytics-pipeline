import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def clean_orders():
    file_path = os.path.join(PROCESSED_PATH, "orders_raw.parquet")
    df = pd.read_parquet(file_path)

    # Standardise column names
    df.columns = df.columns.str.lower().str.strip()

    # Convert date columns
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Remove duplicates
    df = df.drop_duplicates()

    # Save cleaned version
    output_path = os.path.join(PROCESSED_PATH, "orders_clean.parquet")
    df.to_parquet(output_path, index=False)

    print("Orders cleaned successfully.")

if __name__ == "__main__":
    clean_orders()