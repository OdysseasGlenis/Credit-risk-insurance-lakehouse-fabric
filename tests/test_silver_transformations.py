from datetime import datetime

import pytest

from src.common.spark_session import get_spark
from src.local_pipeline.silver_transformations import build_silver_transactions


@pytest.fixture(scope="session")
def spark():
    spark_session = get_spark("pytest_silver_transformations")
    yield spark_session
    spark_session.stop()


def test_silver_transactions_deduplicates_and_flags_orphans(spark):
    raw_transactions = spark.createDataFrame(
        [
            {
                "transaction_id": "T001",
                "customer_id": "C001",
                "card_id": "CARD001",
                "transaction_date": "2025-12-10",
                "year_month": "202512",
                "mcc_code": "5411",
                "merchant_category": "Groceries",
                "transaction_type": "Purchase",
                "amount_eur": "25.50",
                "channel": "POS",
                "ingestion_timestamp": datetime(2025, 12, 10, 10, 0, 0),
                "source_file": "transactions.csv",
                "batch_id": "batch_1",
                "record_hash": "hash_1",
            },
            {
                "transaction_id": "T001",
                "customer_id": "C001",
                "card_id": "CARD001",
                "transaction_date": "2025-12-10",
                "year_month": "202512",
                "mcc_code": "5411",
                "merchant_category": "Groceries",
                "transaction_type": "Purchase",
                "amount_eur": "30.00",
                "channel": "POS",
                "ingestion_timestamp": datetime(2025, 12, 10, 11, 0, 0),
                "source_file": "transactions.csv",
                "batch_id": "batch_2",
                "record_hash": "hash_2",
            },
            {
                "transaction_id": "T002",
                "customer_id": "C999",
                "card_id": "CARD999",
                "transaction_date": "2025-12-11",
                "year_month": "202512",
                "mcc_code": "7995",
                "merchant_category": "Other",
                "transaction_type": "Purchase",
                "amount_eur": "15000.00",
                "channel": "Ecommerce",
                "ingestion_timestamp": datetime(2025, 12, 11, 10, 0, 0),
                "source_file": "transactions.csv",
                "batch_id": "batch_1",
                "record_hash": "hash_3",
            },
        ]
    )

    silver_customers = spark.createDataFrame(
        [
            {"customer_id": "C001"},
        ]
    )

    result = build_silver_transactions(raw_transactions, silver_customers)

    assert result.count() == 2

    latest_t001 = result.filter(result.transaction_id == "T001").collect()[0]
    orphan_t002 = result.filter(result.transaction_id == "T002").collect()[0]

    assert latest_t001.amount_eur == 30.00
    assert orphan_t002.orphan_customer_flag == 1
    assert orphan_t002.suspicious_amount_flag == 1