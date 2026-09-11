import shutil
from pathlib import Path

from src.common.spark_session import get_spark, get_project_root


def read_table(spark, input_path: Path):
    return spark.read.parquet(str(input_path))


def prepare_export_directory(export_path: Path):
    if export_path.exists():
        shutil.rmtree(export_path, ignore_errors=True)

    export_path.mkdir(parents=True, exist_ok=True)


def write_single_csv(df, output_file: Path):
    output_file.parent.mkdir(parents=True, exist_ok=True)

    pandas_df = df.toPandas()
    pandas_df.to_csv(output_file, index=False, encoding="utf-8-sig")


def main():
    spark = get_spark("export_powerbi_datasets")
    root = get_project_root()

    gold_path = root / "lakehouse" / "gold"
    monitoring_path = root / "lakehouse" / "monitoring"
    export_path = root / "exports" / "powerbi"

    print("Starting Power BI dataset export.")

    prepare_export_directory(export_path)

    datasets = [
        {
            "name": "customer_360",
            "input_path": gold_path / "gold_customer_360",
            "output_path": export_path / "customer_360.csv",
        },
        {
            "name": "customer_risk_features",
            "input_path": gold_path / "gold_customer_risk_features",
            "output_path": export_path / "customer_risk_features.csv",
        },
        {
            "name": "monthly_transaction_kpis",
            "input_path": gold_path / "gold_monthly_transaction_kpis",
            "output_path": export_path / "monthly_transaction_kpis.csv",
        },
        {
            "name": "loan_payment_behavior",
            "input_path": gold_path / "gold_loan_payment_behavior",
            "output_path": export_path / "loan_payment_behavior.csv",
        },
        {
            "name": "insurance_claims_mart",
            "input_path": gold_path / "gold_insurance_claims_mart",
            "output_path": export_path / "insurance_claims_mart.csv",
        },
        {
            "name": "dq_results",
            "input_path": monitoring_path / "dq_results",
            "output_path": export_path / "dq_results.csv",
        },
        {
            "name": "pipeline_run_log",
            "input_path": monitoring_path / "pipeline_run_log",
            "output_path": export_path / "pipeline_run_log.csv",
        },
    ]

    for dataset in datasets:
        df = read_table(spark, dataset["input_path"])
        row_count = df.count()

        write_single_csv(df, dataset["output_path"])

        print(
            f"{dataset['name']}: {row_count} rows exported to {dataset['output_path']}"
        )

    print("Power BI dataset export completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()