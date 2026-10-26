# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

sys.path.append("/Workspace/Users/sivana9908_gmail.com#ext#@sivana9908gmail.onmicrosoft.com/Uber-Eats-End-to-End-_Azure-Data-Engineering-Project")
from src.common.spark_utils import standardize_columns
from delta.tables import DeltaTable

# COMMAND ----------

# Environment-specific storage configuration (override the widget per workspace/job).
dbutils.widgets.text("bronze_root", "abfss://bronze@ubereaststorage.dfs.core.windows.net", "Bronze storage root")
BRONZE_ROOT = dbutils.widgets.get("bronze_root").rstrip("/")



# Read the Bronze source.
df_promotions_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BRONZE_ROOT}/csv/promotions/")
)

source_rows = df_promotions_raw.count()
print(f"promotions.source_rows={source_rows}")
display(df_promotions_raw)
df_promotions_raw.printSchema()

# COMMAND ----------

# Standardize column names.
df_promotions = standardize_columns(df_promotions_raw)

# Capture source quality metrics before deduplication and business filtering.
null_promo_ids = df_promotions.filter(col("promo_id").isNull()).count()
null_restaurant_ids = df_promotions.filter(col("restaurant_id").isNull()).count()
duplicate_promotion_keys = (
    df_promotions
    .filter(col("promo_id").isNotNull() & col("restaurant_id").isNotNull())
    .groupBy("promo_id", "restaurant_id")
    .count()
    .filter(col("count") > 1)
    .count()
)
invalid_discount_values = (
    df_promotions
    .filter(col("discount_value").isNull() | (col("discount_value") <= 0))
    .count()
)
missing_promo_codes = df_promotions.filter(
    col("promo_code").isNull() | (trim(col("promo_code")) == "")
).count()

print(f"promotions.quality.null_promo_id_rows={null_promo_ids}")
print(f"promotions.quality.null_restaurant_id_rows={null_restaurant_ids}")
print(f"promotions.quality.duplicate_composite_keys={duplicate_promotion_keys}")
print(f"promotions.quality.invalid_discount_rows={invalid_discount_values}")
print(f"promotions.quality.missing_promo_code_rows={missing_promo_codes}")

# Preserve the current behavior while exposing what is removed. The composite
# key matches the existing Delta MERGE condition.
df_promotions = df_promotions.drop_duplicates(["promo_id", "restaurant_id"])
df_promotions = df_promotions.filter(col("discount_value") > 0)

prepared_duplicate_keys = (
    df_promotions
    .filter(col("promo_id").isNotNull() & col("restaurant_id").isNotNull())
    .groupBy("promo_id", "restaurant_id")
    .count()
    .filter(col("count") > 1)
    .count()
)
if prepared_duplicate_keys:
    raise ValueError(
        "Promotions Silver merge input contains "
        f"{prepared_duplicate_keys} duplicate (promo_id, restaurant_id) keys"
    )

print(f"promotions.quality.prepared_rows={df_promotions.count()}")
print(f"promotions.quality.prepared_duplicate_composite_keys={prepared_duplicate_keys}")
display(df_promotions)

# COMMAND ----------

try:
    table_name = "ubereats_databricks1.silver.promotions"
    
    if not spark.catalog.tableExists(table_name):
        (
            df_promotions.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )
    else:
        target = DeltaTable.forName(spark, table_name)
        (
            target.alias("t")
            .merge(
                df_promotions.alias("s"),
                "t.promo_id = s.promo_id AND t.restaurant_id = s.restaurant_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
except Exception as exc:
    # Record publish failures without replacing the original exception.
    failure_record = [{
        "run_id": str(__import__("uuid").uuid4()),
        "dataset_name": "promotions",
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
        print("promotions.failure_log_error=" + str(log_exc)[:500])
    raise

# Persist a compact successful-run quality record for later monitoring.
quality_table = "ubereats_databricks1.silver.pipeline_quality_results"
quality_record = [{
    "run_id": str(__import__("uuid").uuid4()),
    "dataset_name": "promotions",
    "source_rows": int(source_rows),
    "prepared_rows": int(df_promotions.count()),
    "target_rows_after": int(spark.table(table_name).count()),
    "status": "SUCCEEDED",
    "recorded_at_utc": str(spark.sql("SELECT current_timestamp() AS ts").first()["ts"]),
}]
spark.createDataFrame(quality_record).write.format("delta").mode("append").saveAsTable(quality_table)
print(f"promotions.quality.persisted_run_id=" + quality_record[0]["run_id"])
