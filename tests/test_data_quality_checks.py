import pytest

from src.common.spark_session import get_spark
from src.local_pipeline.data_quality_checks import create_dq_results_df


@pytest.fixture(scope="session")
def spark():
    spark_session = get_spark("pytest_data_quality_checks")
    yield spark_session
    spark_session.stop()


def test_create_dq_results_df_has_expected_rows_and_statuses(spark):
    results = [
        {
            "run_id": "run_1",
            "layer": "silver",
            "table_name": "silver_transactions",
            "check_name": "duplicate_key_transaction_id",
            "check_type": "uniqueness",
            "status": "PASS",
            "failed_records": 0,
            "total_records": 100,
            "threshold": "0",
            "checked_at": "2026-09-10T10:00:00",
            "details": "No duplicate transaction IDs found.",
        },
        {
            "run_id": "run_1",
            "layer": "silver",
            "table_name": "silver_transactions",
            "check_name": "suspicious_amount_records",
            "check_type": "business_rule",
            "status": "WARN",
            "failed_records": 1,
            "total_records": 100,
            "threshold": "0",
            "checked_at": "2026-09-10T10:00:00",
            "details": "Suspicious transaction amount records found.",
        },
    ]

    dq_results_df = create_dq_results_df(spark, results)

    assert dq_results_df.count() == 2
    assert dq_results_df.filter(dq_results_df.status == "PASS").count() == 1
    assert dq_results_df.filter(dq_results_df.status == "WARN").count() == 1