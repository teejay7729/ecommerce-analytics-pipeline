import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = os.path.join(BASE_DIR, "processed_data")

def create_dim_products():
    products = pd.read_parquet(os.path.join(PROCESSED_PATH, "products_clean.parquet"))

    dim_products = products.rename(columns={
        "product_id": "product_id",
        "product_category_name": "category",
        "product_name_lenght": "name_length",
        "product_description_lenght": "description_length",
        "product_photos_qty": "photos_qty",
        "product_weight_g": "weight_g",
        "product_length_cm": "length_cm",
        "product_height_cm": "height_cm",
        "product_width_cm": "width_cm"
    })

    output_path = os.path.join(PROCESSED_PATH, "dim_products.parquet")
    dim_products.to_parquet(output_path, index=False)

    print("dim_products created successfully.")

if __name__ == "__main__":
    create_dim_products()