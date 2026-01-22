import pandas as pd
import random
import os
from faker import Faker
from datetime import datetime

fake = Faker()
VENDORS = ["amazon", "paypal", "flipkart", "blackhawk"]

def generate_data(num_rows=100):
    base_dir = "/opt/airflow/data/landing"
    
    for vendor in VENDORS:
        print(f" Generating data for: {vendor.upper()}...")
        vendor_dir = os.path.join(base_dir, vendor)
        os.makedirs(vendor_dir, exist_ok=True)
        
        data = []
        for _ in range(num_rows):
            row = {
                "transaction_id": fake.uuid4(),
                "customer_id": f"CUST-{random.randint(1000, 1050)}",
                "merchant_id": f"MERCH-{random.randint(1, 20)}",
                "amount": round(random.uniform(10, 5000), 2),
                "transaction_date": fake.date_time_between(start_date='-30d', end_date='now').strftime("%Y-%m-%d"),
                "category": random.choice(['Grocery', 'Electronics', 'Clothing', 'Dining']),
                "source_system": vendor 
            }
            data.append(row)

        df = pd.DataFrame(data)
        file_path = os.path.join(vendor_dir, f"{vendor}_transactions.csv")
        df.to_csv(file_path, index=False)
        print(f" Saved: {file_path}")

if __name__ == "__main__":
    generate_data(50)