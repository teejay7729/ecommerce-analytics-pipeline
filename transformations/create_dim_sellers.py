import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def create_dim_sellers():
    sellers = pd.read_parquet(os.path.join(PROCESSED_PATH, "sellers_clean.parquet"))

    dim_sellers = sellers.rename(columns={
        "seller_id": "seller_id",
        "seller_zip_code_prefix": "zip_code",
        "seller_city": "city",
        "seller_state": "state"
    })

    output_path = os.path.join(PROCESSED_PATH, "dim_sellers.parquet")
    dim_sellers.to_parquet(output_path, index=False)

    print("dim_sellers created successfully.")

if __name__ == "__main__":
    create_dim_sellers()