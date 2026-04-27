# NYC Taxi Databricks Pipeline

Built a Bronze, Silver, Gold data pipeline in Databricks using PySpark and Delta Lake to process NYC taxi trip data.

## Overview

Processes raw NYC taxi trip data into structured, queryable tables for downstream analytics. The pipeline follows a standard lakehouse pattern:

Raw data → Bronze → Silver → Gold

Each layer serves a clear purpose:

- Bronze stores raw data with minimal transformation  
- Silver applies data quality checks and enriches the data  
- Gold contains aggregated tables used for analysis  

## Data Source

NYC Yellow Taxi trip data (Parquet files) and taxi zone lookup data (CSV)

## Pipeline

### Bronze

- Reads raw parquet files from a Databricks Volume  
- Adds ingestion metadata:
  - `ingest_ts`
  - `ingest_date`
  - `source_file`  
- Writes to Delta table:
  - `workspace.default.bronze_yellow_trips`  
- Ingests taxi zone lookup as:
  - `workspace.default.bronze_taxi_zones`  

### Silver

- Applies data quality filters:
  - removes null timestamps  
  - enforces pickup before dropoff  
  - removes invalid distance and fare values  
- Creates derived columns:
  - `trip_duration_min`  
  - `pickup_date`  
  - `pickup_hour`  
- Joins with taxi zone data to add pickup and dropoff borough and zone  
- Writes cleaned fact table:
  - `workspace.default.silver_yellow_trips`  

### Gold

Builds aggregated tables for analysis:

- `gold_daily_borough_metrics`  
  Daily trip count, revenue, and averages by pickup borough  

- `gold_hourly_borough_metrics`  
  Trip volume by hour and pickup borough  

- `gold_tip_metrics`  
  Average tip amount and tip percentage by pickup borough  

## Tech Stack

- Databricks  
- PySpark  
- Delta Lake  
- Unity Catalog  

## Orchestration

The pipeline is designed to run as a Databricks Job with dependent tasks:

1. Bronze ingestion  
2. Silver transformations  
3. Gold aggregations  
