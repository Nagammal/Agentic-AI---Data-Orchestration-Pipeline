import pandas as pd
from rich.console import Console
from rich.table import Table

console = Console()

def validate_dataset(file_path="data/unified_dataset.csv"):
    console.rule("[bold blue] Validation Agent [/bold blue]")
    
    df = pd.read_csv(file_path)
    console.print(f"✅ Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns")

    # -----------------------------
    # 1. Schema Validation
    # -----------------------------
    expected_columns = [
        "loan_id", "customer_id", "loan_amount", "term", "interest_rate",
        "application_date", "status", "age", "income", "employment_status",
        "credit_score", "credit_score_provider", "last_updated"
    ]
    
    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        console.print(f"❌ Missing expected columns: {missing_cols}", style="red")
    else:
        console.print("✅ All expected columns are present")

    # -----------------------------
    # 2. Missing Values Check
    # -----------------------------
    missing_report = df.isnull().sum()
    missing_report = missing_report[missing_report > 0]

    if not missing_report.empty:
        console.print("⚠️ Missing values detected:", style="yellow")
        console.print(missing_report)
    else:
        console.print("✅ No missing values found")

    # -----------------------------
    # 3. Range / Validity Checks
    # -----------------------------
    issues = []
    if "loan_amount" in df.columns and (df["loan_amount"] < 0).any():
        issues.append("Loan amounts contain negative values")

    if "term" in df.columns:
        if not df["term"].between(6, 360).all():  # 6 months – 30 years
            issues.append("Loan terms out of realistic range")
    else:
        issues.append("Column 'term' missing, cannot validate loan duration")

    if "credit_score" in df.columns:
        if not df["credit_score"].dropna().between(300, 850).all():
            issues.append("Credit scores out of FICO range (300–850)")

    if "income" in df.columns and (df["income"] < 0).any():
        issues.append("Income values contain negatives")

    if issues:
        console.print("⚠️ Data quality issues detected:", style="yellow")
        for issue in issues:
            console.print(f"- {issue}")
    else:
        console.print("✅ All numeric values are within valid ranges")

    # -----------------------------
    # 4. Summary Report
    # -----------------------------
    console.rule("[bold green] Validation Summary [/bold green]")

    table = Table(title="Validation Report")
    table.add_column("Check", style="cyan")
    table.add_column("Result", style="magenta")

    table.add_row("Schema", "❌ Issues" if missing_cols else "✅ OK")
    table.add_row("Missing Values", "⚠️ Present" if not missing_report.empty else "✅ None")
    table.add_row("Ranges & Validity", "⚠️ Issues" if issues else "✅ OK")

    console.print(table)

    return {
        "missing_columns": missing_cols,
        "missing_values": missing_report.to_dict(),
        "issues": issues
    }

if __name__ == "__main__":
    report = validate_dataset()
