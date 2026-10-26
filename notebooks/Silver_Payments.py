# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types  import *
from src.common.spark_utils import standardize_columns
from src.common.quality_events import log_pipeline_failure


# COMMAND ----------

# Environment-specific storage configuration (override the widget per workspace/job).
dbutils.widgets.text("bronze_root", "abfss://bronze@ubereaststorage.dfs.core.windows.net", "Bronze storage root")
BRONZE_ROOT = dbutils.widgets.get("bronze_root").rstrip("/")

def fail_validation(message, stage="silver_validation"):
    error = ValueError(message)
    log_pipeline_failure(spark, "payments", stage, error)
    raise error




# MAGIC %md
# MAGIC %-reading data from adls datalake-%

# COMMAND ----------


try:
    df_payments = (
        spark.read
        .format("json")
        .option("multiLine","true")
        .load(f"{BRONZE_ROOT}/api/payments/")
    )
    df_payments_table= df_payments.select(explode("data").alias("payments")).select("payments.*")
except Exception as exc:
    log_pipeline_failure(spark, "payments", "source_read", exc)
    raise

# Guard against an empty API response replacing a previously populated Silver table.
df_payments_table_row_count = df_payments_table.count()
print("payments.source_rows=" + str(df_payments_table_row_count))
if df_payments_table_row_count == 0:
    fail_validation("API Silver input is empty; refusing to overwrite the existing target table")
# Fail before publishing if the API payload has schema, required-field, or key-quality defects.
required_columns = ["payment_id","order_id","payment_method","amount","payment_status","payment_timestamp"]
missing_columns = sorted(set(required_columns) - set(df_payments_table.columns))
if missing_columns:
    fail_validation(f"payments API payload is missing required columns: {missing_columns}")

required_null_conditions = [col(name).isNull() for name in required_columns]
required_null_condition = required_null_conditions[0]
for condition in required_null_conditions[1:]:
    required_null_condition = required_null_condition | condition
required_null_count = df_payments_table.filter(required_null_condition).count()
if required_null_count:
    fail_validation(f"payments API payload has {required_null_count} rows with null required fields: {required_columns}")

key_columns = ["payment_id"]
duplicate_key_count = df_payments_table.groupBy(*key_columns).count().filter(col("count") > 1).count()
if duplicate_key_count:
    fail_validation(f"payments API payload has {duplicate_key_count} duplicate business keys: {key_columns}")

invalid_amount_count = df_payments_table.filter(col("amount").cast("double").isNull() | (col("amount").cast("double") < 0)).count()
if invalid_amount_count:
    fail_validation(f"payments contains {invalid_amount_count} rows with null, nonnumeric, or negative amount")



# COMMAND ----------

# MAGIC %md
# MAGIC # Standardize columns

# COMMAND ----------

# df_customers = standardize_columns(df_customers)
# display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC # Standarize the data types

# COMMAND ----------


# df_customers = df_customers.withColumn("customer_id",col("customer_id").cast("int"))\
#                        .withColumn("signup_date",to_date(col("signup_date")))\
#                            .withColumn("created_at",to_timestamp(col("created_at")))\
#                                    .withColumn("updated_at",to_timestamp(col("updated_at")))
# df_customers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #standardize values

# COMMAND ----------

# df_customers = df_customers.withColumn("city",initcap(col("city")))\
#     .withColumn("status", upper(col("status")))


# COMMAND ----------

# MAGIC %md
# MAGIC #deduplication

# COMMAND ----------

# df_customers=df_customers.dropDuplicates(["customer_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC #handling nulls

# COMMAND ----------

# df_customers = df_customers.filter(col("customer_id").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC # applying business rules

# COMMAND ----------

# # status should contain only these values
# df_customers = df_customers.filter(
#     col("status").isin("ACTIVE", "INACTIVE")
# )

# # Signup date should not be NULL
# df_customers = df_customers.filter(
#     col("signup_date").isNotNull()
# )



# COMMAND ----------

# MAGIC %md
# MAGIC #validations

# COMMAND ----------

# df_customers.groupBy("customer_id").count().filter(col("count")>1).show()
# df_customers.filter(col("customer_id").isNull()).show()
# df_customers.filter(col("status").isNull()).show()


# COMMAND ----------

# MAGIC %md
# MAGIC # creating managed tables for silver cusotler

# COMMAND ----------

try:
    df_payments_table.write.format("delta").mode("overwrite").saveAsTable("ubereats_databricks1.silver.silver_payments")
except Exception as exc:
    # Record publish failures without replacing the original exception.
    failure_record = [{
        "run_id": str(__import__("uuid").uuid4()),
        "dataset_name": "payments",
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
        print("payments.failure_log_error=" + str(log_exc)[:500])
    raise

# Persist a quality record and reconcile the full-refresh row count.
table_name = "ubereats_databricks1.silver.silver_payments"
quality_table = "ubereats_databricks1.silver.pipeline_quality_results"
source_rows = int(df_payments_table_row_count)
prepared_rows = int(df_payments_table.count())
target_rows_after = int(spark.table(table_name).count())
reconciliation_passed = prepared_rows == target_rows_after
quality_record = [{
    "run_id": str(__import__("uuid").uuid4()),
    "dataset_name": "payments",
    "source_rows": source_rows,
    "prepared_rows": prepared_rows,
    "target_rows_after": target_rows_after,
    "status": "SUCCEEDED" if reconciliation_passed else "FAILED_RECONCILIATION",
    "recorded_at_utc": str(spark.sql("SELECT current_timestamp() AS ts").first()["ts"]),
}]
spark.createDataFrame(quality_record).write.format("delta").mode("append").saveAsTable(quality_table)
print("payments.quality.persisted_run_id=" + quality_record[0]["run_id"])
if not reconciliation_passed:
    error = RuntimeError("payments row-count reconciliation failed: prepared_rows=" + str(prepared_rows) + ", target_rows_after=" + str(target_rows_after))
    log_pipeline_failure(spark, "payments", "silver_reconciliation", error)
    raise error
