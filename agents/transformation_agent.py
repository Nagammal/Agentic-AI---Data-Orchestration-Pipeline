# transformation_agent.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, month
from rich.console import Console
from aws_utils import upload_to_s3
import os

console = Console()

def transform_dataset(input_path="data/unified_dataset.csv",
                      output_path="data/transformed_dataset.csv"):
    console.rule("[bold blue] Transformation Agent [/bold blue]")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("AgenticTransformationAgent") \
        .getOrCreate()

    # Load dataset
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    rows, cols = df.count(), len(df.columns)
    console.print(f"✅ Loaded dataset: {rows} rows, {cols} columns")

    # -----------------------------
    # Handle missing values intelligently
    # -----------------------------
    if "credit_score" in df.columns:
        mean_score = df.select("credit_score").na.drop().groupBy().avg().first()[0]
        df = df.withColumn("credit_score", when(col("credit_score").isNull(), mean_score).otherwise(col("credit_score")))
        console.print(f"🔧 Imputed missing credit_score with mean={mean_score}")

    if "income" in df.columns:
        median_income = df.approxQuantile("income", [0.5], 0.0)[0]
        df = df.withColumn("income", when(col("income").isNull(), median_income).otherwise(col("income")))

    # -----------------------------
    # Feature Engineering
    # -----------------------------
    if "loan_amount" in df.columns and "income" in df.columns:
        df = df.withColumn("loan_to_income_ratio", (col("loan_amount") / col("income")))

    if "credit_score" in df.columns:
        df = df.withColumn(
            "risk_segment",
            when(col("credit_score") < 580, "High Risk")
            .when(col("credit_score") < 670, "Medium Risk")
            .otherwise("Low Risk")
        )

    if "application_date" in df.columns:
        df = df.withColumn("application_date", col("application_date").cast("timestamp"))
        df = df.withColumn("application_year", year(col("application_date")))
        df = df.withColumn("application_month", month(col("application_date")))

    # -----------------------------
    # Save locally
    # -----------------------------
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    console.print(f"💾 Transformed dataset saved locally to: {output_path}")

    # -----------------------------
    # Upload to S3 (agentic behavior)
    # -----------------------------
    s3_file = "transformed/transformed_dataset.csv"
    success = upload_to_s3(output_path, s3_file)
    if not success:
        console.print("⚠️ Upload failed. Agent decides to retry or alert.", style="yellow")
    else:
        console.print("✅ Uploaded transformed dataset to S3 successfully.")

    spark.stop()
    return df

if __name__ == "__main__":
    transform_dataset()
