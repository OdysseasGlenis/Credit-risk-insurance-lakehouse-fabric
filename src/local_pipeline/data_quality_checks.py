import uuid
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.spark_session import get_spark, get_project_root


def read_table(spark, input_path: Path):
    return spark.read.parquet(str(input_path))


def write_table(df, output_path: Path):
    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )


def add_result(
    results,
    run_id,
    layer,
    table_name,
    check_name,
    check_type,
    status,
    failed_records,
    total_records,
    threshold,
    details,
):
    results.append(
        {
            "run_id": run_id,
            "layer": layer,
            "table_name": table_name,
            "check_name": check_name,
            "check_type": check_type,
            "status": status,
            "failed_records": int(failed_records),
            "total_records": int(total_records),
            "threshold": threshold,
            "checked_at": datetime.utcnow().isoformat(),
            "details": details,
        }
    )


def check_min_row_count(results, run_id, layer, table_name, df, min_expected_rows):
    total_records = df.count()
    failed_records = 0 if total_records >= min_expected_rows else 1
    status = "PASS" if failed_records == 0 else "FAIL"

    add_result(
        results=results,
        run_id=run_id,
        layer=layer,
        table_name=table_name,
        check_name="minimum_row_count",
        check_type="completeness",
        status=status,
        failed_records=failed_records,
        total_records=total_records,
        threshold=f">= {min_expected_rows}",
        details=f"Expected at least {min_expected_rows} rows, found {total_records}.",
    )

    return total_records


def check_null_key(results, run_id, layer, table_name, df, key_col):
    total_records = df.count()
    failed_records = df.filter(F.col(key_col).isNull() | (F.trim(F.col(key_col)) == "")).count()
    status = "PASS" if failed_records == 0 else "FAIL"

    add_result(
        results=results,
        run_id=run_id,
        layer=layer,
        table_name=table_name,
        check_name=f"null_or_blank_{key_col}",
        check_type="validity",
        status=status,
        failed_records=failed_records,
        total_records=total_records,
        threshold="0",
        details=f"Found {failed_records} records with null or blank {key_col}.",
    )


def check_duplicate_key(results, run_id, layer, table_name, df, key_cols, status_when_found="FAIL"):
    total_records = df.count()

    duplicate_keys = (
        df
        .groupBy(*key_cols)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_keys == 0:
        status = "PASS"
    else:
        status = status_when_found

    add_result(
        results=results,
        run_id=run_id,
        layer=layer,
        table_name=table_name,
        check_name=f"duplicate_key_{'_'.join(key_cols)}",
        check_type="uniqueness",
        status=status,
        failed_records=duplicate_keys,
        total_records=total_records,
        threshold="0",
        details=f"Found {duplicate_keys} duplicate key groups for {key_cols}.",
    )

    return duplicate_keys


def check_orphan_records(
    results,
    run_id,
    layer,
    table_name,
    child_df,
    child_key,
    parent_df,
    parent_key,
    check_name,
    status_when_found="FAIL",
):
    total_records = child_df.count()

    orphan_records = (
        child_df
        .filter(F.col(child_key).isNotNull())
        .join(
            parent_df.select(F.col(parent_key).alias("_parent_key")).distinct(),
            F.col(child_key) == F.col("_parent_key"),
            "left_anti",
        )
        .count()
    )

    status = "PASS" if orphan_records == 0 else status_when_found

    add_result(
        results=results,
        run_id=run_id,
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        check_type="referential_integrity",
        status=status,
        failed_records=orphan_records,
        total_records=total_records,
        threshold="0",
        details=f"Found {orphan_records} orphan records where {child_key} does not exist in parent {parent_key}.",
    )

    return orphan_records


def check_condition_count(
    results,
    run_id,
    layer,
    table_name,
    df,
    check_name,
    check_type,
    condition,
    status_when_found,
    details_prefix,
):
    total_records = df.count()
    failed_records = df.filter(condition).count()

    if failed_records == 0:
        status = "PASS"
    else:
        status = status_when_found

    add_result(
        results=results,
        run_id=run_id,
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        check_type=check_type,
        status=status,
        failed_records=failed_records,
        total_records=total_records,
        threshold="0",
        details=f"{details_prefix}: {failed_records} records.",
    )

    return failed_records


def create_dq_results_df(spark, results):
    schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("layer", T.StringType(), False),
        T.StructField("table_name", T.StringType(), False),
        T.StructField("check_name", T.StringType(), False),
        T.StructField("check_type", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("failed_records", T.LongType(), False),
        T.StructField("total_records", T.LongType(), False),
        T.StructField("threshold", T.StringType(), True),
        T.StructField("checked_at", T.StringType(), False),
        T.StructField("details", T.StringType(), True),
    ])

    return spark.createDataFrame(results, schema=schema)


def create_pipeline_run_log_df(
    spark,
    run_id,
    started_at,
    ended_at,
    dq_results_df,
    bronze_table_count,
    silver_table_count,
    gold_table_count,
):
    failed_checks_count = dq_results_df.filter(F.col("status") == "FAIL").count()
    warning_checks_count = dq_results_df.filter(F.col("status") == "WARN").count()
    total_checks_count = dq_results_df.count()

    if failed_checks_count > 0:
       pipeline_status = "FAILED_WITH_DQ_ERRORS"
    elif warning_checks_count > 0:
       pipeline_status = "SUCCESS_WITH_WARNINGS"
    else:
       pipeline_status = "SUCCESS"

    schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("pipeline_name", T.StringType(), False),
        T.StructField("started_at", T.StringType(), False),
        T.StructField("ended_at", T.StringType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("bronze_table_count", T.IntegerType(), False),
        T.StructField("silver_table_count", T.IntegerType(), False),
        T.StructField("gold_table_count", T.IntegerType(), False),
        T.StructField("dq_checks_count", T.LongType(), False),
        T.StructField("failed_checks_count", T.LongType(), False),
        T.StructField("warning_checks_count", T.LongType(), False),
    ])

    data = [
        {
            "run_id": run_id,
            "pipeline_name": "local_bronze_silver_gold_dq_pipeline",
            "started_at": started_at,
            "ended_at": ended_at,
            "status": pipeline_status,
            "bronze_table_count": bronze_table_count,
            "silver_table_count": silver_table_count,
            "gold_table_count": gold_table_count,
            "dq_checks_count": total_checks_count,
            "failed_checks_count": failed_checks_count,
            "warning_checks_count": warning_checks_count,
        }
    ]

    return spark.createDataFrame(data, schema=schema)


def main():
    spark = get_spark("data_quality_checks")
    root = get_project_root()

    bronze_path = root / "lakehouse" / "bronze"
    silver_path = root / "lakehouse" / "silver"
    gold_path = root / "lakehouse" / "gold"
    monitoring_path = root / "lakehouse" / "monitoring"

    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow().isoformat()
    results = []

    print(f"Starting Data Quality checks. run_id={run_id}")

    bronze_customers = read_table(spark, bronze_path / "bronze_customers")
    bronze_transactions = read_table(spark, bronze_path / "bronze_transactions")

    silver_customers = read_table(spark, silver_path / "silver_customers")
    silver_accounts = read_table(spark, silver_path / "silver_accounts")
    silver_cards = read_table(spark, silver_path / "silver_cards")
    silver_transactions = read_table(spark, silver_path / "silver_transactions")
    silver_loans = read_table(spark, silver_path / "silver_loans")
    silver_claims = read_table(spark, silver_path / "silver_claims")
    silver_customer_notes = read_table(spark, silver_path / "silver_customer_notes")

    gold_monthly_transaction_kpis = read_table(spark, gold_path / "gold_monthly_transaction_kpis")
    gold_customer_360 = read_table(spark, gold_path / "gold_customer_360")
    gold_customer_risk_features = read_table(spark, gold_path / "gold_customer_risk_features")

    bronze_customers_count = check_min_row_count(
        results, run_id, "bronze", "bronze_customers", bronze_customers, 1
    )
    bronze_transactions_count = check_min_row_count(
        results, run_id, "bronze", "bronze_transactions", bronze_transactions, 1
    )

    silver_customers_count = check_min_row_count(
        results, run_id, "silver", "silver_customers", silver_customers, 1
    )
    silver_transactions_count = check_min_row_count(
        results, run_id, "silver", "silver_transactions", silver_transactions, 1
    )

    gold_customer_360_count = check_min_row_count(
        results, run_id, "gold", "gold_customer_360", gold_customer_360, 1
    )
    gold_risk_count = check_min_row_count(
        results, run_id, "gold", "gold_customer_risk_features", gold_customer_risk_features, 1
    )

    check_null_key(results, run_id, "silver", "silver_customers", silver_customers, "customer_id")
    check_null_key(results, run_id, "silver", "silver_transactions", silver_transactions, "transaction_id")
    check_null_key(results, run_id, "silver", "silver_transactions", silver_transactions, "customer_id")

    bronze_duplicate_transaction_keys = check_duplicate_key(
        results,
        run_id,
        "bronze",
        "bronze_transactions",
        bronze_transactions,
        ["transaction_id"],
        status_when_found="WARN",
    )

    check_duplicate_key(
        results,
        run_id,
        "silver",
        "silver_transactions",
        silver_transactions,
        ["transaction_id"],
    )

    check_duplicate_key(
        results,
        run_id,
        "gold",
        "gold_customer_360",
        gold_customer_360,
        ["customer_id"],
    )

    check_duplicate_key(
        results,
        run_id,
        "gold",
        "gold_customer_risk_features",
        gold_customer_risk_features,
        ["customer_id"],
    )

    check_orphan_records(
        results=results,
        run_id=run_id,
        layer="silver",
        table_name="silver_transactions",
        child_df=silver_transactions,
        child_key="customer_id",
        parent_df=silver_customers,
        parent_key="customer_id",
        check_name="orphan_transactions_to_customers",
        status_when_found="WARN",
    )

    check_orphan_records(
        results=results,
        run_id=run_id,
        layer="silver",
        table_name="silver_accounts",
        child_df=silver_accounts,
        child_key="customer_id",
        parent_df=silver_customers,
        parent_key="customer_id",
        check_name="orphan_accounts_to_customers",
    )

    check_orphan_records(
        results=results,
        run_id=run_id,
        layer="silver",
        table_name="silver_cards",
        child_df=silver_cards,
        child_key="customer_id",
        parent_df=silver_customers,
        parent_key="customer_id",
        check_name="orphan_cards_to_customers",
    )

    check_orphan_records(
        results=results,
        run_id=run_id,
        layer="silver",
        table_name="silver_loans",
        child_df=silver_loans,
        child_key="customer_id",
        parent_df=silver_customers,
        parent_key="customer_id",
        check_name="orphan_loans_to_customers",
    )

    check_orphan_records(
        results=results,
        run_id=run_id,
        layer="silver",
        table_name="silver_claims",
        child_df=silver_claims,
        child_key="customer_id",
        parent_df=silver_customers,
        parent_key="customer_id",
        check_name="orphan_claims_to_customers",
    )

    check_orphan_records(
        results=results,
        run_id=run_id,
        layer="silver",
        table_name="silver_customer_notes",
        child_df=silver_customer_notes,
        child_key="customer_id",
        parent_df=silver_customers,
        parent_key="customer_id",
        check_name="orphan_notes_to_customers",
    )

    check_condition_count(
        results=results,
        run_id=run_id,
        layer="silver",
        table_name="silver_transactions",
        df=silver_transactions,
        check_name="suspicious_amount_records",
        check_type="business_rule",
        condition=(F.col("suspicious_amount_flag") == 1),
        status_when_found="WARN",
        details_prefix="Suspicious transaction amount records found",
    )

    check_condition_count(
        results=results,
        run_id=run_id,
        layer="gold",
        table_name="gold_customer_risk_features",
        df=gold_customer_risk_features,
        check_name="invalid_risk_score",
        check_type="business_rule",
        condition=(
            F.col("risk_score").isNull()
            | (F.col("risk_score") < 0)
            | (F.col("risk_score") > 100)
        ),
        status_when_found="FAIL",
        details_prefix="Invalid risk score records found",
    )

    check_condition_count(
        results=results,
        run_id=run_id,
        layer="gold",
        table_name="gold_customer_risk_features",
        df=gold_customer_risk_features,
        check_name="invalid_risk_band",
        check_type="business_rule",
        condition=(
            F.col("risk_band").isNull()
            | (~F.col("risk_band").isin("Low", "Medium", "High"))
        ),
        status_when_found="FAIL",
        details_prefix="Invalid risk band records found",
    )

    expected_silver_transaction_count = bronze_transactions_count - bronze_duplicate_transaction_keys
    reconciliation_failed = 0 if silver_transactions_count == expected_silver_transaction_count else 1

    add_result(
        results=results,
        run_id=run_id,
        layer="silver",
        table_name="silver_transactions",
        check_name="bronze_to_silver_transaction_reconciliation",
        check_type="reconciliation",
        status="PASS" if reconciliation_failed == 0 else "FAIL",
        failed_records=reconciliation_failed,
        total_records=silver_transactions_count,
        threshold=f"expected {expected_silver_transaction_count}",
        details=(
            f"bronze_transactions={bronze_transactions_count}, "
            f"bronze_duplicate_transaction_keys={bronze_duplicate_transaction_keys}, "
            f"expected_silver_transactions={expected_silver_transaction_count}, "
            f"actual_silver_transactions={silver_transactions_count}."
        ),
    )

    customer_360_failed = 0 if gold_customer_360_count == silver_customers_count else 1

    add_result(
        results=results,
        run_id=run_id,
        layer="gold",
        table_name="gold_customer_360",
        check_name="customer_360_to_silver_customer_count",
        check_type="reconciliation",
        status="PASS" if customer_360_failed == 0 else "FAIL",
        failed_records=customer_360_failed,
        total_records=gold_customer_360_count,
        threshold=f"expected {silver_customers_count}",
        details=(
            f"silver_customers={silver_customers_count}, "
            f"gold_customer_360={gold_customer_360_count}."
        ),
    )

    risk_features_failed = 0 if gold_risk_count == gold_customer_360_count else 1

    add_result(
        results=results,
        run_id=run_id,
        layer="gold",
        table_name="gold_customer_risk_features",
        check_name="risk_features_to_customer_360_count",
        check_type="reconciliation",
        status="PASS" if risk_features_failed == 0 else "FAIL",
        failed_records=risk_features_failed,
        total_records=gold_risk_count,
        threshold=f"expected {gold_customer_360_count}",
        details=(
            f"gold_customer_360={gold_customer_360_count}, "
            f"gold_customer_risk_features={gold_risk_count}."
        ),
    )

    dq_results_df = create_dq_results_df(spark, results)

    ended_at = datetime.utcnow().isoformat()

    pipeline_run_log_df = create_pipeline_run_log_df(
        spark=spark,
        run_id=run_id,
        started_at=started_at,
        ended_at=ended_at,
        dq_results_df=dq_results_df,
        bronze_table_count=2,
        silver_table_count=7,
        gold_table_count=3,
    )

    write_table(dq_results_df, monitoring_path / "dq_results")
    write_table(pipeline_run_log_df, monitoring_path / "pipeline_run_log")

    print("DQ summary:")
    dq_results_df.groupBy("status").count().orderBy("status").show(truncate=False)

    print("Failed/Warn checks:")
    dq_results_df.filter(F.col("status").isin("FAIL", "WARN")).select(
        "layer",
        "table_name",
        "check_name",
        "status",
        "failed_records",
        "details",
    ).show(truncate=False)

    print("Pipeline run log:")
    pipeline_run_log_df.show(truncate=False)

    print("Data Quality checks completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()