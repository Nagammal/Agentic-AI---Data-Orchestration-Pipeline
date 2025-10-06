import pandas as pd
import sqlite3
import requests
from rich.console import Console

console = Console()

def ingest_data():
    console.rule("[bold blue] Ingestion Agent [/bold blue]")

    # -----------------------------
    # 1. Load loans (CSV)
    # -----------------------------
    loans = pd.read_csv("data/loans.csv")
    console.print(f"✅ Loans Data Loaded: {loans.shape}")

    # -----------------------------
    # 2. Load customers (SQLite DB)
    # -----------------------------
    conn = sqlite3.connect("data/customers.db")
    customers = pd.read_sql_query("SELECT * FROM customers", conn)
    conn.close()
    console.print(f"✅ Customers Data Loaded: {customers.shape}")

    # -----------------------------
    # 3. Fetch credit scores (API)
    # -----------------------------
    credit_scores = []
    for cust_id in customers["customer_id"]:
        url = f"http://127.0.0.1:8000/credit-score/{cust_id}"
        try:
            resp = requests.get(url, timeout=2)
            if resp.status_code == 200:
                credit_scores.append(resp.json())
            else:
                console.print(f"⚠️ Failed to fetch score for {cust_id}")
        except Exception as e:
            console.print(f"⚠️ API error for {cust_id}: {e}")

    credit_scores_df = pd.DataFrame(credit_scores)
    console.print(f"✅ Credit Scores Loaded: {credit_scores_df.shape}")

    # -----------------------------
    # 4. Merge datasets
    # -----------------------------
    merged = (
        loans.merge(customers, on="customer_id", how="left")
             .merge(credit_scores_df, on="customer_id", how="left")
    )

    console.print(f"✅ Final Merged Dataset: {merged.shape}")
    console.print(merged.head())

    # Save unified dataset
    merged.to_csv("data/unified_dataset.csv", index=False)
    console.print("💾 Unified dataset saved to data/unified_dataset.csv")

    return merged

if __name__ == "__main__":
    ingest_data()
