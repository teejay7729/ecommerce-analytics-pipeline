import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def clean_sellers():
    file_path = os.path.join(PROCESSED_PATH, "sellers_raw.parquet")
    df = pd.read_parquet(file_path)

    df.columns = df.columns.str.lower().str.strip()
    df = df.drop_duplicates()

    output_path = os.path.join(PROCESSED_PATH, "sellers_clean.parquet")
    df.to_parquet(output_path, index=False)

    print("Sellers cleaned successfully.")

if __name__ == "__main__":
    clean_sellers()