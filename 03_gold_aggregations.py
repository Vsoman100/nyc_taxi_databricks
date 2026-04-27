# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver_df = spark.table("workspace.default.silver_yellow_trips")

# COMMAND ----------

# When is demand highest?
gold_daily = (
    silver_df
    .groupBy("pickup_date", "PU_borough")
    .agg(
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_fare"),
        F.avg("trip_distance_mi").alias("avg_trip_distance_mi"),
        F.avg("trip_duration_min").alias("avg_trip_duration_min")
    )
)

# COMMAND ----------

gold_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_daily_borough_metrics")

# COMMAND ----------

# When are peak hours?
gold_hourly = (
    silver_df
    .groupBy("pickup_hour", "PU_borough")
    .agg(
        F.count("*").alias("trip_count")
    )
)

# COMMAND ----------

gold_hourly.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_hourly_borough_metrics")

# COMMAND ----------

# Which areas give the best tips?
gold_tips = (
    silver_df
    .withColumn(
        "tip_pct",
        F.when(F.col("total_amount") > 0,
               F.col("tip_amount") / F.col("total_amount"))
    )
    .groupBy("PU_borough")
    .agg(
        F.avg("tip_amount").alias("avg_tip"),
        F.avg("tip_pct").alias("avg_tip_pct")
    )
)

# COMMAND ----------

gold_tips.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.gold_tip_metrics")