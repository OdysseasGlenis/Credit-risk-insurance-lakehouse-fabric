# 01_bronze_ingestion_audit.py

from pyspark.sql import functions as F
import uuid

batch_id = str(uuid.uuid4())

def add_bronze_metadata(df, source_file):
    return (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(source_file))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("record_hash", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in df.columns]), 256))
    )

# Example in Fabric:
# df = spark.read.option("header", True).csv("Files/raw/customers.csv")
# add_bronze_metadata(df, "customers.csv").write.mode("overwrite").format("delta").saveAsTable("bronze_customers")
