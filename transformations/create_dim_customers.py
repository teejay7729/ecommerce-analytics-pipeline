import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def create_dim_customers():
    customers = pd.read_parquet(os.path.join(PROCESSED_PATH, "customers_clean.parquet"))

    dim_customers = customers.rename(columns={
        "customer_id": "customer_id",
        "customer_unique_id": "customer_unique_id",
        "customer_zip_code_prefix": "zip_code",
        "customer_city": "city",
        "customer_state": "state"
    })

    output_path = os.path.join(PROCESSED_PATH, "dim_customers.parquet")
    dim_customers.to_parquet(output_path, index=False)

    print("dim_customers created successfully.")

if __name__ == "__main__":
    create_dim_customers()