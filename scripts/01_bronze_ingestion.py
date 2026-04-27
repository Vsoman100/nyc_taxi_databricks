# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

raw_path = "/Volumes/workspace/default/nyc_taxi_raw/yellow_raw/"
bronze_table = "workspace.default.bronze_yellow_trips"

# COMMAND ----------

# Bronze layer: ingest raw parquet files and append ingestion metadata.
bronze_df = (
    spark.read
        .parquet(raw_path)
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingest_date", F.to_date("ingest_ts"))
)

# COMMAND ----------

# Load in zone metadata
zones_path = "/Volumes/workspace/default/nyc_taxi_raw/taxi_zone_lookup.csv"

zones_df = (
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zones_path)
      .select(
          F.col("LocationID").cast("int").alias("LocationID"),
          F.col("Borough").alias("borough"),
          F.col("Zone").alias("zone"),
          F.col("service_zone").alias("service_zone")
      )
)

# COMMAND ----------

# Keep raw trips as a Delta table for downstream Silver transformations.
(
    bronze_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("ingest_date")
        .saveAsTable(bronze_table)
)


# COMMAND ----------

# Same for zones
zones_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_taxi_zones")
