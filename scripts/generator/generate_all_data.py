import pandas as pd
import random
import uuid
import os
import sys
from datetime import datetime, timedelta
from faker import Faker

# --- CONFIGURATION ---
NUM_TRANSACTIONS = 1000000 
NUM_CUSTOMERS = 10000
NUM_MERCHANTS = 500
OUTPUT_DIR = "../../data/raw"

fake = Faker()

def setup_directories():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"[INFO] Created directory: {OUTPUT_DIR}")

def generate_customers():
    print(f"[INFO] Generating {NUM_CUSTOMERS} Customers...")
    data = []
    for _ in range(NUM_CUSTOMERS):
        data.append({
            "customer_id": str(uuid.uuid4()),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date='-2y', end_date='today')
        })
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, "dim_customers.csv"), index=False)
    print(f"[SUCCESS] Saved dim_customers.csv")
    return df['customer_id'].tolist()

def generate_merchants():
    print(f"[INFO] Generating {NUM_MERCHANTS} Merchants...")
    categories = ['Grocery', 'Dining', 'Travel', 'Electronics', 'Retail', 'Fuel']
    data = []
    for _ in range(NUM_MERCHANTS):
        data.append({
            "merchant_id": str(uuid.uuid4()),
            "merchant_name": fake.company(),
            "category": random.choice(categories),
            "city": fake.city(),
            "state": fake.state_abbr()
        })
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(OUTPUT_DIR, "dim_merchants.csv"), index=False)
    print(f"[SUCCESS] Saved dim_merchants.csv")
    return df

def generate_reward_rules():
    print("[INFO] Generating Reward Rules...")
    rules = [
        {"category": "Grocery", "min_spend": 50, "reward_points": 10, "multiplier": 1.0},
        {"category": "Dining", "min_spend": 20, "reward_points": 5, "multiplier": 1.5},
        {"category": "Travel", "min_spend": 100, "reward_points": 50, "multiplier": 2.0},
        {"category": "Electronics", "min_spend": 200, "reward_points": 30, "multiplier": 1.2},
        {"category": "Retail", "min_spend": 40, "reward_points": 5, "multiplier": 1.0}
    ]
    pd.DataFrame(rules).to_csv(os.path.join(OUTPUT_DIR, "ref_reward_rules.csv"), index=False)
    print(f"[SUCCESS] Saved ref_reward_rules.csv")

def generate_date_dimension():
    print("[INFO] Generating Date Dimension...")
    start, end = datetime(2024, 1, 1), datetime(2026, 12, 31)
    dates = []
    curr = start
    while curr <= end:
        dates.append({
            "date_key": int(curr.strftime('%Y%m%d')),
            "full_date": curr.date(),
            "year": curr.year,
            "month": curr.month,
            "day_of_week": curr.strftime('%A'),
            "is_weekend": curr.weekday() >= 5
        })
        curr += timedelta(days=1)
    pd.DataFrame(dates).to_csv(os.path.join(OUTPUT_DIR, "dim_date.csv"), index=False)
    print(f"[SUCCESS] Saved dim_date.csv")

def generate_transactions(cust_ids, merch_df):
    print(f"[INFO] Generating {NUM_TRANSACTIONS} Transactions...")
    merch_ids = merch_df['merchant_id'].tolist()
    data = []
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(NUM_TRANSACTIONS):
        if i % 100000 == 0 and i > 0: print(f"   ... {i} rows")
        data.append({
            "transaction_id": str(uuid.uuid4()),
            "customer_id": random.choice(cust_ids),
            "merchant_id": random.choice(merch_ids),
            "transaction_date": fake.date_between(start_date='-1y', end_date='today'),
            "amount": round(random.uniform(5.00, 1000.00), 2),
            "currency": "USD"
        })
    
    df = pd.DataFrame(data)
    # Add duplicates for testing
    df = pd.concat([df, df.sample(frac=0.02)])
    df.to_csv(os.path.join(OUTPUT_DIR, "fact_transactions.csv"), index=False)
    print(f"[SUCCESS] Saved fact_transactions.csv")

if __name__ == "__main__":
    setup_directories()
    cust_ids = generate_customers()
    merch_df = generate_merchants()
    generate_reward_rules()
    generate_date_dimension()
    generate_transactions(cust_ids, merch_df)
    print("\n[COMPLETE] All data generated!")