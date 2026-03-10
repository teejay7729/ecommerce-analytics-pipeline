import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def create_fact_orders():
    orders = pd.read_parquet(os.path.join(PROCESSED_PATH, "orders_clean.parquet"))

    fact_orders = orders.rename(columns={
        "order_id": "order_id",
        "customer_id": "customer_id",
        "order_status": "status",
        "order_purchase_timestamp": "purchase_ts",
        "order_approved_at": "approved_ts",
        "order_delivered_carrier_date": "carrier_ts",
        "order_delivered_customer_date": "delivered_ts",
        "order_estimated_delivery_date": "estimated_ts"
    })

    output_path = os.path.join(PROCESSED_PATH, "fact_orders.parquet")
    fact_orders.to_parquet(output_path, index=False)

    print("fact_orders created successfully.")

if __name__ == "__main__":
    create_fact_orders()