from pathlib import Path

from pyspark.sql import functions as F

from src.common.spark_session import get_spark, get_project_root


def read_table(spark, input_path: Path):
    return spark.read.parquet(str(input_path))


def write_table(df, output_path: Path):
    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )


def build_monthly_transaction_kpis(transactions):
    return (
        transactions
        .groupBy("customer_id", "year_month")
        .agg(
            F.count("*").alias("transaction_count"),
            F.countDistinct("mcc_code").alias("distinct_mcc_count"),
            F.round(
                F.sum(
                    F.when(F.col("amount_eur") > 0, F.col("amount_eur"))
                     .otherwise(F.lit(0.0))
                ),
                2
            ).alias("total_spend_eur"),
            F.round(F.avg("amount_eur"), 2).alias("avg_transaction_amount_eur"),
            F.round(F.max("amount_eur"), 2).alias("max_transaction_amount_eur"),
            F.sum(F.when(F.col("channel") == "POS", 1).otherwise(0)).alias("pos_tx_count"),
            F.sum(F.when(F.col("channel") == "Ecommerce", 1).otherwise(0)).alias("ecommerce_tx_count"),
            F.sum(F.when(F.col("channel") == "ATM", 1).otherwise(0)).alias("atm_tx_count"),
            F.sum(F.when(F.col("transaction_type") == "Refund", 1).otherwise(0)).alias("refund_tx_count"),
            F.sum("suspicious_amount_flag").alias("suspicious_tx_count"),
            F.max("orphan_customer_flag").alias("orphan_customer_flag"),
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def build_transaction_window_features(transactions):
    as_of_date = F.to_date(F.lit("2025-12-31"))
    start_l3m = F.add_months(F.trunc(as_of_date, "MM"), -2)
    start_l6m = F.add_months(F.trunc(as_of_date, "MM"), -5)
    start_l12m = F.add_months(F.trunc(as_of_date, "MM"), -11)

    return (
        transactions
        .filter(F.col("orphan_customer_flag") == 0)
        .groupBy("customer_id")
        .agg(
            F.round(
                F.sum(
                    F.when(
                        (F.col("transaction_date") >= start_l3m) & (F.col("amount_eur") > 0),
                        F.col("amount_eur")
                    ).otherwise(0.0)
                ),
                2
            ).alias("total_spend_l3m"),
            F.round(
                F.sum(
                    F.when(
                        (F.col("transaction_date") >= start_l6m) & (F.col("amount_eur") > 0),
                        F.col("amount_eur")
                    ).otherwise(0.0)
                ),
                2
            ).alias("total_spend_l6m"),
            F.round(
                F.sum(
                    F.when(
                        (F.col("transaction_date") >= start_l12m) & (F.col("amount_eur") > 0),
                        F.col("amount_eur")
                    ).otherwise(0.0)
                ),
                2
            ).alias("total_spend_l12m"),
            F.sum(
                F.when(F.col("transaction_date") >= start_l3m, 1).otherwise(0)
            ).alias("transaction_count_l3m"),
            F.sum(
                F.when(F.col("transaction_date") >= start_l6m, 1).otherwise(0)
            ).alias("transaction_count_l6m"),
            F.sum(
                F.when(F.col("transaction_date") >= start_l12m, 1).otherwise(0)
            ).alias("transaction_count_l12m"),
            F.sum(
                F.when(
                    (F.col("transaction_date") >= start_l12m) & (F.col("suspicious_amount_flag") == 1),
                    1
                ).otherwise(0)
            ).alias("suspicious_tx_count_l12m"),
            F.sum(
                F.when(
                    (F.col("transaction_date") >= start_l12m) & (F.col("channel") == "Ecommerce"),
                    1
                ).otherwise(0)
            ).alias("ecommerce_tx_count_l12m"),
        )
        .withColumn(
            "avg_monthly_spend_l6m",
            F.round(F.col("total_spend_l6m") / F.lit(6), 2)
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def build_credit_card_features(cards):
    return (
        cards
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_cards_count"),
            F.sum("active_card_flag").alias("active_cards_count"),
            F.sum("credit_card_flag").alias("credit_cards_count"),
            F.sum(
                F.when(
                    (F.col("credit_card_flag") == 1) & (F.col("active_card_flag") == 1),
                    1
                ).otherwise(0)
            ).alias("active_credit_cards_count"),
            F.round(
                F.sum(
                    F.when(
                        (F.col("card_type") == "Credit") & (F.col("card_status") == "ACTIVE"),
                        F.col("credit_limit_eur")
                    ).otherwise(0.0)
                ),
                2
            ).alias("total_active_credit_limit_eur"),
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def build_account_features(accounts):
    return (
        accounts
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_accounts_count"),
            F.sum("active_account_flag").alias("active_accounts_count"),
            F.round(
                F.sum(
                    F.when(F.col("account_status") == "ACTIVE", F.col("balance_eur"))
                     .otherwise(0.0)
                ),
                2
            ).alias("total_active_balance_eur"),
            F.round(F.avg("balance_eur"), 2).alias("avg_account_balance_eur"),
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def build_loan_payment_behavior(loans):
    return (
        loans
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_loans_count"),
            F.sum(F.when(F.col("loan_status") == "ACTIVE", 1).otherwise(0)).alias("active_loans_count"),
            F.round(F.sum("current_balance_eur"), 2).alias("total_current_loan_balance_eur"),
            F.round(F.sum("monthly_installment_eur"), 2).alias("total_monthly_installment_eur"),
            F.max("days_past_due").alias("max_days_past_due"),
            F.sum("delinquent_30_plus_flag").alias("delinquent_30_plus_count"),
            F.sum("delinquent_60_plus_flag").alias("delinquent_60_plus_count"),
            F.max("default_flag").alias("has_default_flag"),
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def build_insurance_claims_mart(claims):
    return (
        claims
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("claims_count"),
            F.sum(F.when(F.col("claim_status") == "PAID", 1).otherwise(0)).alias("paid_claims_count"),
            F.sum(F.when(F.col("claim_status") == "APPROVED", 1).otherwise(0)).alias("approved_claims_count"),
            F.sum(F.when(F.col("claim_status") == "REJECTED", 1).otherwise(0)).alias("rejected_claims_count"),
            F.round(F.sum("claim_amount_eur"), 2).alias("total_claim_amount_eur"),
            F.round(F.avg("claim_amount_eur"), 2).alias("avg_claim_amount_eur"),
            F.sum(F.when(F.col("claim_severity") == "high", 1).otherwise(0)).alias("high_severity_claims_count"),
            F.sum(F.when(F.col("claim_has_documents_flag") == 0, 1).otherwise(0)).alias("claims_without_documents_count"),
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def build_customer_note_features(customer_notes):
    return (
        customer_notes
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("customer_notes_count"),
            F.max("financial_stress_flag").alias("financial_stress_flag"),
            F.max("complaint_flag").alias("complaint_flag"),
            F.max("settlement_request_flag").alias("settlement_request_flag"),
            F.max("fraud_alert_flag").alias("fraud_alert_flag"),
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def build_customer_360(
    customers,
    account_features,
    credit_card_features,
    loan_payment_behavior,
    insurance_claims_mart,
    customer_note_features,
    transaction_window_features,
):
    customer_360 = (
        customers
        .select(
            "customer_id",
            "gender",
            "birth_year",
            "age",
            "city",
            "segment",
            "employment_status",
            "customer_since",
            "declared_income_eur",
            "declared_income_missing_flag",
            "declared_income_eur_clean",
        )
        .join(account_features.drop("gold_processed_timestamp"), on="customer_id", how="left")
        .join(credit_card_features.drop("gold_processed_timestamp"), on="customer_id", how="left")
        .join(loan_payment_behavior.drop("gold_processed_timestamp"), on="customer_id", how="left")
        .join(insurance_claims_mart.drop("gold_processed_timestamp"), on="customer_id", how="left")
        .join(customer_note_features.drop("gold_processed_timestamp"), on="customer_id", how="left")
        .join(transaction_window_features.drop("gold_processed_timestamp"), on="customer_id", how="left")
    )

    numeric_fill_cols = [
        "total_accounts_count",
        "active_accounts_count",
        "total_active_balance_eur",
        "avg_account_balance_eur",
        "total_cards_count",
        "active_cards_count",
        "credit_cards_count",
        "active_credit_cards_count",
        "total_active_credit_limit_eur",
        "total_loans_count",
        "active_loans_count",
        "total_current_loan_balance_eur",
        "total_monthly_installment_eur",
        "max_days_past_due",
        "delinquent_30_plus_count",
        "delinquent_60_plus_count",
        "has_default_flag",
        "claims_count",
        "paid_claims_count",
        "approved_claims_count",
        "rejected_claims_count",
        "total_claim_amount_eur",
        "avg_claim_amount_eur",
        "high_severity_claims_count",
        "claims_without_documents_count",
        "customer_notes_count",
        "financial_stress_flag",
        "complaint_flag",
        "settlement_request_flag",
        "fraud_alert_flag",
        "total_spend_l3m",
        "total_spend_l6m",
        "total_spend_l12m",
        "transaction_count_l3m",
        "transaction_count_l6m",
        "transaction_count_l12m",
        "suspicious_tx_count_l12m",
        "ecommerce_tx_count_l12m",
        "avg_monthly_spend_l6m",
    ]

    fill_dict = {col_name: 0 for col_name in numeric_fill_cols if col_name in customer_360.columns}

    return (
        customer_360
        .fillna(fill_dict)
        .withColumn(
            "credit_utilization_proxy_l3m",
            F.when(
                F.col("total_active_credit_limit_eur") > 0,
                F.round(F.col("total_spend_l3m") / F.col("total_active_credit_limit_eur"), 4)
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "debt_to_income_proxy",
            F.when(
                F.col("declared_income_eur_clean") > 0,
                F.round(F.col("total_monthly_installment_eur") * 12 / F.col("declared_income_eur_clean"), 4)
            ).otherwise(F.lit(0.0))
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def build_customer_risk_features(customer_360):
    risk_score_raw = (
        F.when(F.col("declared_income_missing_flag") == 1, 10).otherwise(0)
        + F.when(F.col("financial_stress_flag") == 1, 20).otherwise(0)
        + F.when(F.col("complaint_flag") == 1, 5).otherwise(0)
        + F.when(F.col("fraud_alert_flag") == 1, 15).otherwise(0)
        + F.when(F.col("has_default_flag") == 1, 35).otherwise(0)
        + F.when(F.col("delinquent_60_plus_count") > 0, 25).otherwise(0)
        + F.when(F.col("delinquent_30_plus_count") > 0, 15).otherwise(0)
        + F.when(F.col("suspicious_tx_count_l12m") > 0, 15).otherwise(0)
        + F.when(F.col("credit_utilization_proxy_l3m") > 0.8, 15).otherwise(0)
        + F.when(F.col("debt_to_income_proxy") > 0.5, 15).otherwise(0)
        + F.when(F.col("high_severity_claims_count") > 0, 10).otherwise(0)
    )

    return (
        customer_360
        .withColumn("risk_score", F.least(risk_score_raw, F.lit(100)))
        .withColumn(
            "risk_band",
            F.when(F.col("risk_score") >= 70, "High")
             .when(F.col("risk_score") >= 40, "Medium")
             .otherwise("Low")
        )
        .withColumn(
            "high_risk_customer_flag",
            F.when(F.col("risk_score") >= 70, 1).otherwise(0)
        )
        .withColumn("gold_processed_timestamp", F.current_timestamp())
    )


def main():
    spark = get_spark("gold_features")
    root = get_project_root()

    silver_path = root / "lakehouse" / "silver"
    gold_path = root / "lakehouse" / "gold"

    print("Starting Gold feature generation.")

    customers = read_table(spark, silver_path / "silver_customers")
    accounts = read_table(spark, silver_path / "silver_accounts")
    cards = read_table(spark, silver_path / "silver_cards")
    transactions = read_table(spark, silver_path / "silver_transactions")
    loans = read_table(spark, silver_path / "silver_loans")
    claims = read_table(spark, silver_path / "silver_claims")
    customer_notes = read_table(spark, silver_path / "silver_customer_notes")

    monthly_transaction_kpis = build_monthly_transaction_kpis(transactions)
    transaction_window_features = build_transaction_window_features(transactions)
    account_features = build_account_features(accounts)
    credit_card_features = build_credit_card_features(cards)
    loan_payment_behavior = build_loan_payment_behavior(loans)
    insurance_claims_mart = build_insurance_claims_mart(claims)
    customer_note_features = build_customer_note_features(customer_notes)

    customer_360 = build_customer_360(
        customers,
        account_features,
        credit_card_features,
        loan_payment_behavior,
        insurance_claims_mart,
        customer_note_features,
        transaction_window_features,
    )

    customer_risk_features = build_customer_risk_features(customer_360)

    outputs = [
        ("gold_monthly_transaction_kpis", monthly_transaction_kpis),
        ("gold_credit_card_features", credit_card_features),
        ("gold_loan_payment_behavior", loan_payment_behavior),
        ("gold_insurance_claims_mart", insurance_claims_mart),
        ("gold_customer_note_features", customer_note_features),
        ("gold_customer_360", customer_360),
        ("gold_customer_risk_features", customer_risk_features),
    ]

    for table_name, df in outputs:
        output_path = gold_path / table_name
        write_table(df, output_path)
        print(f"{table_name}: {df.count()} rows written to {output_path}")

    print("Gold feature generation completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()