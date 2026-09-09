from pathlib import Path
import uuid

from pyspark.sql import functions as F

from src.common.spark_session import get_spark, get_project_root


def add_bronze_metadata(df, source_file: str, batch_id: str):
    original_cols = df.columns

    return (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(source_file))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws("||", *[F.col(c).cast("string") for c in original_cols]),
                256
            )
        )
    )


def read_source(spark, file_format: str, path: Path):
    if file_format == "csv":
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(path))
        )

    if file_format == "json":
        return spark.read.json(str(path))

    raise ValueError(f"Unsupported file format: {file_format}")


def write_table(df, output_path: Path):
    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )


def main():
    spark = get_spark("bronze_ingestion")
    root = get_project_root()

    raw_path = root / "data" / "raw"
    reference_path = root / "data" / "reference"
    bronze_path = root / "lakehouse" / "bronze"

    batch_id = str(uuid.uuid4())

    sources = [
        {
            "table_name": "bronze_customers",
            "file_format": "csv",
            "path": raw_path / "customers.csv",
        },
        {
            "table_name": "bronze_accounts",
            "file_format": "csv",
            "path": raw_path / "accounts.csv",
        },
        {
            "table_name": "bronze_cards",
            "file_format": "json",
            "path": raw_path / "credit_cards.json",
        },
        {
            "table_name": "bronze_transactions",
            "file_format": "csv",
            "path": raw_path / "transactions.csv",
        },
        {
            "table_name": "bronze_loans",
            "file_format": "csv",
            "path": raw_path / "loans.csv",
        },
        {
            "table_name": "bronze_claims",
            "file_format": "json",
            "path": raw_path / "insurance_claims.json",
        },
        {
            "table_name": "bronze_customer_notes",
            "file_format": "csv",
            "path": raw_path / "customer_notes.csv",
        },
        {
            "table_name": "bronze_product_mapping",
            "file_format": "csv",
            "path": reference_path / "product_mapping.csv",
        },
        {
            "table_name": "bronze_dates_mapping",
            "file_format": "csv",
            "path": reference_path / "dates_mapping.csv",
        },
    ]

    print(f"Starting Bronze ingestion. batch_id={batch_id}")

    for source in sources:
        table_name = source["table_name"]
        path = source["path"]

        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {path}")

        df = read_source(spark, source["file_format"], path)
        df_bronze = add_bronze_metadata(df, path.name, batch_id)

        output_path = bronze_path / table_name
        write_table(df_bronze, output_path)

        row_count = df_bronze.count()
        print(f"{table_name}: {row_count} rows written to {output_path}")

    print("Bronze ingestion completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()