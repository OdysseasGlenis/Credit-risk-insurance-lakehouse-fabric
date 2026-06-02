# 03_gold_risk_features.py

from pyspark.sql import functions as F

# Example:
# tx = spark.table("silver_transactions")
# gold_monthly_transaction_kpis = (
#     tx.groupBy("customer_id", "year_month")
#       .agg(
#           F.sum(F.when(F.col("amount_eur") > 0, F.col("amount_eur")).otherwise(0)).alias("total_spend_eur"),
#           F.count("*").alias("transaction_count"),
#           F.avg("amount_eur").alias("avg_transaction_amount_eur")
#       )
# )
