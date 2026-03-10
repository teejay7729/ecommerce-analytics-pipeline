import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def create_fact_order_items():
    items = pd.read_parquet(os.path.join(PROCESSED_PATH, "order_items_clean.parquet"))

    fact_items = items.rename(columns={
        "order_id": "order_id",
        "order_item_id": "item_number",
        "product_id": "product_id",
        "seller_id": "seller_id",
        "shipping_limit_date": "shipping_limit",
        "price": "price",
        "freight_value": "freight"
    })

    output_path = os.path.join(PROCESSED_PATH, "fact_order_items.parquet")
    fact_items.to_parquet(output_path, index=False)

    print("fact_order_items created successfully.")

if __name__ == "__main__":
    create_fact_order_items()