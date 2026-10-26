# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.common.spark_utils import standardize_columns
from delta.tables import DeltaTable

# COMMAND ----------

# Environment-specific storage configuration (override the widget per workspace/job).
dbutils.widgets.text("bronze_root", "abfss://bronze@ubereaststorage.dfs.core.windows.net", "Bronze storage root")
BRONZE_ROOT = dbutils.widgets.get("bronze_root").rstrip("/")



# Read the Bronze source.
df_drivers_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BRONZE_ROOT}/csv/drivers/")
)

source_rows = df_drivers_raw.count()
print(f"drivers.source_rows={source_rows}")
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

try:
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
except Exception as exc:
    # Log publish failures separately without hiding the original exception.
    failure_record = [{
        "run_id": str(__import__("uuid").uuid4()),
        "dataset_name": "drivers",
        "stage": "silver_publish",
        "error_type": type(exc).__name__,
        "error_message": str(exc)[:2000],
        "recorded_at_utc": str(spark.sql("SELECT current_timestamp() AS ts").first()["ts"]),
    }]
    try:
        spark.createDataFrame(failure_record).write.format("delta").mode("append").saveAsTable(
            "ubereats_databricks1.silver.pipeline_failure_events"
        )
    except Exception as log_exc:
        print("drivers.failure_log_error=" + str(log_exc)[:500])
    raise

# Persist a compact successful-run quality record for later monitoring.
quality_table = "ubereats_databricks1.silver.pipeline_quality_results"
quality_record = [{
    "run_id": str(__import__("uuid").uuid4()),
    "dataset_name": "drivers",
    "source_rows": int(source_rows),
    "prepared_rows": int(df_drivers.count()),
    "target_rows_after": int(spark.table(table_name).count()),
    "status": "SUCCEEDED",
    "recorded_at_utc": str(spark.sql("SELECT current_timestamp() AS ts").first()["ts"]),
}]
spark.createDataFrame(quality_record).write.format("delta").mode("append").saveAsTable(quality_table)
print(f"drivers.quality.persisted_run_id=" + quality_record[0]["run_id"])
