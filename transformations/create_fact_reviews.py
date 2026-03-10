import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def create_fact_reviews():
    reviews = pd.read_parquet(os.path.join(PROCESSED_PATH, "reviews_clean.parquet"))

    fact_reviews = reviews.rename(columns={
        "review_id": "review_id",
        "order_id": "order_id",
        "review_score": "score",
        "review_comment_title": "title",
        "review_comment_message": "message",
        "review_creation_date": "created",
        "review_answer_timestamp": "answered"
    })

    output_path = os.path.join(PROCESSED_PATH, "fact_reviews.parquet")
    fact_reviews.to_parquet(output_path, index=False)

    print("fact_reviews created successfully.")

if __name__ == "__main__":
    create_fact_reviews()