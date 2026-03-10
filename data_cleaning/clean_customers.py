import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def clean_customers():
    file_path = os.path.join(PROCESSED_PATH, "customers_raw.parquet")
    df = pd.read_parquet(file_path)

    # Standardise column names
    df.columns = df.columns.str.lower().str.strip()

    # Remove duplicates
    df = df.drop_duplicates()

    # Save cleaned version
    output_path = os.path.join(PROCESSED_PATH, "customers_clean.parquet")
    df.to_parquet(output_path, index=False)

    print("Customers cleaned successfully.")

if __name__ == "__main__":
    clean_customers()