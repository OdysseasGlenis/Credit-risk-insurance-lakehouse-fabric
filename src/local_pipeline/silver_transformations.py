from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from src.common.spark_session import get_spark, get_project_root


def read_table(spark, input_path: Path):
    return spark.read.parquet(str(input_path))


def write_table(df, output_path: Path):
    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )


def build_silver_customers(df):
    as_of_date = F.to_date(F.lit("2025-12-31"))

    return (
        df
        .select(
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.upper(F.trim(F.col("gender"))).alias("gender"),
            F.col("birth_year").cast("int").alias("birth_year"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("segment")).alias("segment"),
            F.trim(F.col("employment_status")).alias("employment_status"),
            F.to_date(F.col("customer_since")).alias("customer_since"),
            F.col("declared_income_eur").cast("double").alias("declared_income_eur"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .withColumn("age", F.year(as_of_date) - F.col("birth_year"))
        .withColumn(
            "declared_income_missing_flag",
            F.when(F.col("declared_income_eur").isNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "declared_income_eur_clean",
            F.coalesce(F.col("declared_income_eur"), F.lit(0.0))
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def build_silver_accounts(df):
    return (
        df
        .select(
            F.trim(F.col("account_id")).alias("account_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("account_type")).alias("account_type"),
            F.to_date(F.col("open_date")).alias("open_date"),
            F.upper(F.trim(F.col("status"))).alias("account_status"),
            F.col("balance_eur").cast("double").alias("balance_eur"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .withColumn(
            "active_account_flag",
            F.when(F.col("account_status") == "ACTIVE", F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def build_silver_cards(df):
    return (
        df
        .select(
            F.trim(F.col("card_id")).alias("card_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.initcap(F.trim(F.col("card_type"))).alias("card_type"),
            F.upper(F.trim(F.col("card_status"))).alias("card_status"),
            F.to_date(F.col("issue_date")).alias("issue_date"),
            F.col("credit_limit_eur").cast("double").alias("credit_limit_eur"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .withColumn(
            "active_card_flag",
            F.when(F.col("card_status") == "ACTIVE", F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "credit_card_flag",
            F.when(F.col("card_type") == "Credit", F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def build_silver_transactions(df, silver_customers):
    w = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_timestamp").desc())

    valid_customers = (
        silver_customers
        .select("customer_id")
        .distinct()
        .withColumn("valid_customer_flag", F.lit(1))
    )

    tx_deduped = (
        df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    return (
        tx_deduped
        .select(
            F.trim(F.col("transaction_id")).alias("transaction_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("card_id")).alias("card_id"),
            F.to_date(F.col("transaction_date")).alias("transaction_date"),
            F.col("year_month").cast("int").alias("year_month"),
            F.trim(F.col("mcc_code")).alias("mcc_code"),
            F.trim(F.col("merchant_category")).alias("merchant_category"),
            F.trim(F.col("transaction_type")).alias("transaction_type"),
            F.col("amount_eur").cast("double").alias("amount_eur"),
            F.trim(F.col("channel")).alias("channel"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .join(valid_customers, on="customer_id", how="left")
        .withColumn(
            "orphan_customer_flag",
            F.when(F.col("valid_customer_flag").isNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "suspicious_amount_flag",
            F.when(
                (F.col("amount_eur") < -1000) | (F.col("amount_eur") > 10000),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .drop("valid_customer_flag")
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def build_silver_loans(df):
    return (
        df
        .select(
            F.trim(F.col("loan_id")).alias("loan_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("loan_type")).alias("loan_type"),
            F.to_date(F.col("open_date")).alias("open_date"),
            F.col("original_principal_eur").cast("double").alias("original_principal_eur"),
            F.col("current_balance_eur").cast("double").alias("current_balance_eur"),
            F.col("monthly_installment_eur").cast("double").alias("monthly_installment_eur"),
            F.col("days_past_due").cast("int").alias("days_past_due"),
            F.upper(F.trim(F.col("loan_status"))).alias("loan_status"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .withColumn(
            "delinquent_30_plus_flag",
            F.when(F.col("days_past_due") >= 30, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "delinquent_60_plus_flag",
            F.when(F.col("days_past_due") >= 60, F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "default_flag",
            F.when(F.col("loan_status") == "DEFAULT", F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def build_silver_claims(df):
    claim_details_schema = T.StructType([
        T.StructField("source", T.StringType(), True),
        T.StructField("severity", T.StringType(), True),
        T.StructField("has_documents", T.BooleanType(), True),
    ])

    return (
        df
        .select(
            F.trim(F.col("claim_id")).alias("claim_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.to_date(F.col("claim_date")).alias("claim_date"),
            F.trim(F.col("claim_type")).alias("claim_type"),
            F.col("claim_amount_eur").cast("double").alias("claim_amount_eur"),
            F.upper(F.trim(F.col("claim_status"))).alias("claim_status"),
            F.col("claim_details_json"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .withColumn("claim_details", F.from_json(F.col("claim_details_json"), claim_details_schema))
        .withColumn("claim_source", F.col("claim_details.source"))
        .withColumn("claim_severity", F.col("claim_details.severity"))
        .withColumn("claim_has_documents_flag", F.col("claim_details.has_documents").cast("int"))
        .drop("claim_details")
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def build_silver_customer_notes(df):
    note_text_lower = F.lower(F.col("note_text"))

    return (
        df
        .select(
            F.trim(F.col("note_id")).alias("note_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.to_date(F.col("note_date")).alias("note_date"),
            F.trim(F.col("note_text")).alias("note_text"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .withColumn(
            "financial_stress_flag",
            F.when(
                note_text_lower.rlike("financial stress|lost job|restructuring|missed payment"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "complaint_flag",
            F.when(
                note_text_lower.rlike("complaint|fees|late charge"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "settlement_request_flag",
            F.when(
                note_text_lower.rlike("settlement|early loan repayment"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "fraud_alert_flag",
            F.when(
                note_text_lower.rlike("fraud"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def build_silver_product_mapping(df):
    return (
        df
        .select(
            F.trim(F.col("product_code")).alias("product_code"),
            F.trim(F.col("product_group")).alias("product_group"),
            F.col("risk_weight").cast("double").alias("risk_weight"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def build_silver_dates_mapping(df):
    return (
        df
        .select(
            F.col("year_month").cast("int").alias("year_month"),
            F.to_date(F.col("period_start_date")).alias("period_start_date"),
            F.to_date(F.col("period_end_date")).alias("period_end_date"),
            F.col("measurement_period_id").cast("int").alias("measurement_period_id"),
            F.col("ingestion_timestamp"),
            F.col("source_file"),
            F.col("batch_id"),
            F.col("record_hash"),
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )


def main():
    spark = get_spark("silver_transformations")
    root = get_project_root()

    bronze_path = root / "lakehouse" / "bronze"
    silver_path = root / "lakehouse" / "silver"

    print("Starting Silver transformations.")

    bronze_customers = read_table(spark, bronze_path / "bronze_customers")
    bronze_accounts = read_table(spark, bronze_path / "bronze_accounts")
    bronze_cards = read_table(spark, bronze_path / "bronze_cards")
    bronze_transactions = read_table(spark, bronze_path / "bronze_transactions")
    bronze_loans = read_table(spark, bronze_path / "bronze_loans")
    bronze_claims = read_table(spark, bronze_path / "bronze_claims")
    bronze_customer_notes = read_table(spark, bronze_path / "bronze_customer_notes")
    bronze_product_mapping = read_table(spark, bronze_path / "bronze_product_mapping")
    bronze_dates_mapping = read_table(spark, bronze_path / "bronze_dates_mapping")

    silver_customers = build_silver_customers(bronze_customers)
    silver_accounts = build_silver_accounts(bronze_accounts)
    silver_cards = build_silver_cards(bronze_cards)
    silver_transactions = build_silver_transactions(bronze_transactions, silver_customers)
    silver_loans = build_silver_loans(bronze_loans)
    silver_claims = build_silver_claims(bronze_claims)
    silver_customer_notes = build_silver_customer_notes(bronze_customer_notes)
    silver_product_mapping = build_silver_product_mapping(bronze_product_mapping)
    silver_dates_mapping = build_silver_dates_mapping(bronze_dates_mapping)

    outputs = [
        ("silver_customers", silver_customers),
        ("silver_accounts", silver_accounts),
        ("silver_cards", silver_cards),
        ("silver_transactions", silver_transactions),
        ("silver_loans", silver_loans),
        ("silver_claims", silver_claims),
        ("silver_customer_notes", silver_customer_notes),
        ("silver_product_mapping", silver_product_mapping),
        ("silver_dates_mapping", silver_dates_mapping),
    ]

    for table_name, df in outputs:
        output_path = silver_path / table_name
        write_table(df, output_path)
        print(f"{table_name}: {df.count()} rows written to {output_path}")

    print("Silver transformations completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()