import pytest

from src.common.spark_session import get_spark
from src.local_pipeline.gold_features import build_customer_risk_features


@pytest.fixture(scope="session")
def spark():
    spark_session = get_spark("pytest_gold_features")
    yield spark_session
    spark_session.stop()


def test_customer_risk_features_create_valid_score_and_band(spark):
    customer_360 = spark.createDataFrame(
        [
            {
                "customer_id": "C001",
                "declared_income_missing_flag": 0,
                "financial_stress_flag": 0,
                "complaint_flag": 0,
                "fraud_alert_flag": 0,
                "has_default_flag": 0,
                "delinquent_60_plus_count": 0,
                "delinquent_30_plus_count": 0,
                "suspicious_tx_count_l12m": 0,
                "credit_utilization_proxy_l3m": 0.20,
                "debt_to_income_proxy": 0.10,
                "high_severity_claims_count": 0,
            },
            {
                "customer_id": "C002",
                "declared_income_missing_flag": 1,
                "financial_stress_flag": 1,
                "complaint_flag": 1,
                "fraud_alert_flag": 1,
                "has_default_flag": 1,
                "delinquent_60_plus_count": 1,
                "delinquent_30_plus_count": 1,
                "suspicious_tx_count_l12m": 1,
                "credit_utilization_proxy_l3m": 0.95,
                "debt_to_income_proxy": 0.80,
                "high_severity_claims_count": 1,
            },
        ]
    )

    result = build_customer_risk_features(customer_360)

    assert result.count() == 2

    invalid_scores = result.filter(
        (result.risk_score < 0) | (result.risk_score > 100)
    ).count()

    invalid_bands = result.filter(
        ~result.risk_band.isin("Low", "Medium", "High")
    ).count()

    high_risk_count = result.filter(result.high_risk_customer_flag == 1).count()

    assert invalid_scores == 0
    assert invalid_bands == 0
    assert high_risk_count == 1