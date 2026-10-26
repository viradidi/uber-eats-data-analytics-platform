# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

sys.path.append("/Workspace/Users/sivana9908_gmail.com#ext#@sivana9908gmail.onmicrosoft.com/Uber-Eats-End-to-End-_Azure-Data-Engineering-Project")
from src.common.spark_utils import standardize_columns
from delta.tables import DeltaTable

# COMMAND ----------

# Read the Bronze source.
df_drivers_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("abfss://bronze@ubereaststorage.dfs.core.windows.net/csv/drivers/")
)

print(f"drivers.source_rows={df_drivers_raw.count()}")
display(df_drivers_raw)
df_drivers_raw.printSchema()

# COMMAND ----------

# Standardize column names and selected categorical values.
df_drivers = standardize_columns(df_drivers_raw)
df_drivers = (
    df_drivers
    .withColumn("vehicle_type", upper(trim(col("vehicle_type"))))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("city", initcap(trim(col("city"))))
)

# Record quality metrics BEFORE filtering or deduplication. These counts make
# source issues visible without changing the notebook's existing row handling.
null_driver_ids = df_drivers.filter(col("driver_id").isNull()).count()
duplicate_driver_keys = (
    df_drivers
    .filter(col("driver_id").isNotNull())
    .groupBy("driver_id")
    .count()
    .filter(col("count") > 1)
    .count()
)
missing_join_dates = df_drivers.filter(col("join_date").isNull()).count()
invalid_vehicle_types = (
    df_drivers
    .filter(col("vehicle_type").isNull() | ~col("vehicle_type").isin("SCOOTER", "BIKE"))
    .count()
)
invalid_statuses = (
    df_drivers
    .filter(col("status").isNull() | ~col("status").isin("ACTIVE", "INACTIVE"))
    .count()
)

print(f"drivers.quality.null_driver_id_rows={null_driver_ids}")
print(f"drivers.quality.duplicate_driver_id_keys={duplicate_driver_keys}")
print(f"drivers.quality.missing_join_date_rows={missing_join_dates}")
print(f"drivers.quality.invalid_vehicle_type_rows={invalid_vehicle_types}")
print(f"drivers.quality.invalid_status_rows={invalid_statuses}")

# Preserve the existing transformation rules: remove null IDs, deduplicate by
# driver_id, and keep supported vehicle types. Metrics above show what was
# present before those operations.
df_drivers = df_drivers.filter(col("driver_id").isNotNull())
df_drivers = df_drivers.drop_duplicates(["driver_id"])
df_drivers = df_drivers.filter(col("vehicle_type").isin("SCOOTER", "BIKE"))

# Validate the prepared merge input. A MERGE source must have unique keys.
prepared_duplicate_keys = (
    df_drivers
    .groupBy("driver_id")
    .count()
    .filter(col("count") > 1)
    .count()
)
if prepared_duplicate_keys:
    raise ValueError(
        f"Drivers Silver merge input contains {prepared_duplicate_keys} duplicate driver_id keys"
    )

print(f"drivers.quality.prepared_rows={df_drivers.count()}")
print(f"drivers.quality.prepared_duplicate_driver_id_keys={prepared_duplicate_keys}")

# COMMAND ----------

table_name = "ubereats_databricks1.silver.drivers"

if not spark.catalog.tableExists(table_name):
    (
        df_drivers.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )
else:
    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("t")
        .merge(
            df_drivers.alias("s"),
            "t.driver_id = s.driver_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
