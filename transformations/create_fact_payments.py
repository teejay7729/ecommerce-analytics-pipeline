import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def create_fact_payments():
    payments = pd.read_parquet(os.path.join(PROCESSED_PATH, "payments_clean.parquet"))

    fact_payments = payments.rename(columns={
        "order_id": "order_id",
        "payment_sequential": "payment_seq",
        "payment_type": "payment_type",
        "payment_installments": "installments",
        "payment_value": "value"
    })

    output_path = os.path.join(PROCESSED_PATH, "fact_payments.parquet")
    fact_payments.to_parquet(output_path, index=False)

    print("fact_payments created successfully.")

if __name__ == "__main__":
    create_fact_payments()