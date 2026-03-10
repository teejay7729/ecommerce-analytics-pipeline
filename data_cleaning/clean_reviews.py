import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def clean_reviews():
    file_path = os.path.join(PROCESSED_PATH, "reviews_raw.parquet")
    df = pd.read_parquet(file_path)

    # Standardise column names
    df.columns = df.columns.str.lower().str.strip()

    # Convert review creation and answer timestamps
    date_cols = ["review_creation_date", "review_answer_timestamp"]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # Remove duplicates
    df = df.drop_duplicates()

    # Save cleaned version
    output_path = os.path.join(PROCESSED_PATH, "reviews_clean.parquet")
    df.to_parquet(output_path, index=False)

    print("Reviews cleaned successfully.")

if __name__ == "__main__":
    clean_reviews()