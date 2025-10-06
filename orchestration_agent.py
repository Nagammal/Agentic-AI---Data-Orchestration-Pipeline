# orchestration_agent.py
from agents.ingestion import ingest_data
from agents.validation_agent import validate_dataset
from agents.transformation_agent import transform_dataset
from rich.console import Console
from rich.panel import Panel

console = Console()

def run_pipeline():
    console.rule("[bold blue] Orchestration Agent [/bold blue]")

    try:
        # -----------------------------
        # 1. Ingestion
        # -----------------------------
        console.print("📥 Starting Ingestion Agent...")
        unified_df = ingest_data()  # Returns merged DataFrame
        console.print(f"✅ Ingestion complete: {unified_df.shape[0]} rows, {unified_df.shape[1]} columns")

        # -----------------------------
        # 2. Validation
        # -----------------------------
        console.print("\n🔍 Starting Validation Agent...")
        validation_report = validate_dataset()
        console.print("✅ Validation complete")

        # Agentic behavior: decide whether to proceed
        if validation_report.get("issues_count", 0) > 0:
            console.print("⚠️ Data quality issues detected. Agents decide to proceed with caution.", style="yellow")
        else:
            console.print("✅ Data passed validation. Proceeding to transformation.")

        # -----------------------------
        # 3. Transformation
        # -----------------------------
        console.print("\n⚙️ Starting Transformation Agent...")
        transform_dataset()  # Handles local save + S3 upload
        console.print("✅ Transformation complete")

        # -----------------------------
        # 4. Pipeline Summary
        # -----------------------------
        console.rule("[bold green] Pipeline Completed Successfully [/bold green]")
        console.print(Panel.fit(
            f"📥 Ingested: {unified_df.shape}\n"
            f"🔍 Validation Issues: {validation_report.get('issues_count', 0)} found\n"
            f"⚙️ Transformed & Uploaded to S3\n",
            title="Pipeline Summary",
            border_style="green"
        ))

    except Exception as e:
        console.rule("[bold red] Pipeline Failed [/bold red]")
        console.print(f"❌ Error: {e}", style="red")

if __name__ == "__main__":
    run_pipeline()
