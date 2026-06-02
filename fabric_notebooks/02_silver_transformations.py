# 02_silver_transformations.py

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Example:
# w = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_timestamp").desc())
# silver_transactions = (
#     spark.table("bronze_transactions")
#     .withColumn("rn", F.row_number().over(w))
#     .filter("rn = 1")
#     .drop("rn")
#     .withColumn("transaction_date", F.to_date("transaction_date"))
#     .withColumn("year_month", F.col("year_month").cast("int"))
#     .withColumn("amount_eur", F.col("amount_eur").cast("double"))
# )
