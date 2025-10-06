#Creates data/loans.csv and data/customers.db (SQLite) with sample rows.
# generate_data.py
import os
import sqlite3
import random
from datetime import datetime, timedelta
import pandas as pd

os.makedirs("data", exist_ok=True)

NUM_CUSTOMERS = 300
NUM_LOANS = 800

# 1) customers table (demographics)
customers = []
cities = ["New York", "San Francisco", "Chicago", "Austin", "Seattle"]
employment = ["Employed", "Self-Employed", "Unemployed", "Retired"]

for i in range(1, NUM_CUSTOMERS + 1):
    cid = f"CUST{str(i).zfill(4)}"
    age = random.randint(21, 75)
    income = random.randint(20000, 250000)
    customers.append((cid, age, random.choice(["M", "F"]), income, random.choice(employment), random.choice(cities)))

conn = sqlite3.connect("data/customers.db")
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    age INTEGER,
    gender TEXT,
    income INTEGER,
    employment_status TEXT,
    city TEXT
)
""")
cur.executemany("INSERT OR REPLACE INTO customers VALUES (?,?,?,?,?,?)", customers)
conn.commit()
conn.close()

# 2) loans CSV
loans = []
for i in range(1, NUM_LOANS + 1):
    loan_id = f"LOAN{str(i).zfill(6)}"
    cust = random.choice(customers)[0]
    loan_amount = round(random.uniform(2000, 500000), 2)
    term_months = random.choice([12, 24, 36, 48, 60])
    interest_rate = round(random.uniform(3.5, 24.0), 2)
    application_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
    status = random.choices(["APPROVED", "REJECTED", "PENDING"], weights=[0.8, 0.05, 0.15])[0]
    defaulted = random.choices([0,1], weights=[0.95, 0.05])[0]
    loans.append((loan_id, cust, loan_amount, term_months, interest_rate, application_date, status, defaulted))

loans_df = pd.DataFrame(loans, columns=[
    "loan_id", "customer_id", "loan_amount", "term_months", "interest_rate", "application_date", "status", "defaulted"
])
loans_df.to_csv("data/loans.csv", index=False)

print("Generated data/customers.db and data/loans.csv")
