# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

silver_table = "workspace.default.silver_yellow_trips"

# COMMAND ----------

# Loading bronze_df back into spark from delta table
bronze_df = spark.table("workspace.default.bronze_yellow_trips")

# COMMAND ----------

# Double checking schema
bronze_df.printSchema()

# COMMAND ----------

# Loading bronze_df back into spark from delta table
zones_df = spark.table("workspace.default.bronze_taxi_zones")

# COMMAND ----------

# Apply basic data quality filters to remove invalid trips
cleaned_df = (
    bronze_df
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
        .filter(F.col("trip_distance") > F.lit(0))
        .filter(F.col("fare_amount") >= F.lit(0))
)

# COMMAND ----------

# Checking how many rows got removed
before = bronze_df.count()
after  = cleaned_df.count()
print(f"Bronze rows: {before:,}")
print(f"Silver-pass rows: {after:,} ({after/before:.2%})")

# COMMAND ----------

enriched = (
    cleaned_df
        .withColumn("trip_duration_min", (F.col("tpep_dropoff_datetime").cast("timestamp").cast("double") - F.col("tpep_pickup_datetime").cast("timestamp").cast("double")) / 60.0) # in minutes
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
)

# COMMAND ----------

pu = (
    zones_df.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("borough").alias("PU_borough"),
        F.col("service_zone").alias("PU_service_zone"),
        F.col("zone").alias("PU_zone")
    )
)

do = (
    zones_df.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("borough").alias("DO_borough"),
        F.col("service_zone").alias("DO_service_zone"),
        F.col("zone").alias("DO_zone")
    )
)

silver_df = (
    enriched
        .join(pu, on="PULocationID", how="left")
        .join(do, on="DOLocationID", how="left")
)

# COMMAND ----------

silver_fact = silver_df.select(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_date",
    "pickup_hour",
    "PULocationID",
    "DOLocationID",
    "PU_borough",
    "PU_zone",
    "DO_borough",
    "DO_zone",
    "passenger_count",
    F.col("trip_distance").alias("trip_distance_mi"),
    "trip_duration_min",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "payment_type",
    "RatecodeID",
    "store_and_fwd_flag",
    "ingest_ts",
    "ingest_date",
    "source_file"
)

# COMMAND ----------

(
    silver_fact.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("pickup_date")
        .saveAsTable(silver_table)
)