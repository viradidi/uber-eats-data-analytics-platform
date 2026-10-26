# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types  import *
import sys
sys.path.append("/Workspace/Users/sivana9908_gmail.com#ext#@sivana9908gmail.onmicrosoft.com/Uber-Eats-End-to-End-_Azure-Data-Engineering-Project")
from src.common.spark_utils import standardize_columns


# COMMAND ----------

# MAGIC %md
# MAGIC %-reading data from adls datalake-%

# COMMAND ----------


df_payments = (
    spark.read
    .format("json")
    .option("multiLine","true")
    .load("abfss://bronze@ubereaststorage.dfs.core.windows.net/api/payments/")
)
df_payments_table= df_payments.select(explode("data").alias("payments")).select("payments.*")



# Guard against an empty API response replacing a previously populated Silver table.
df_payments_table_row_count = df_payments_table.count()
print("payments.source_rows=" + str(df_payments_table_row_count))
if df_payments_table_row_count == 0:
    raise ValueError("API Silver input is empty; refusing to overwrite the existing target table")

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

df_payments_table.write.format("delta").mode("overwrite").saveAsTable("ubereats_databricks1.silver.silver_payments")

# Persist successful-run quality metrics after the Silver write succeeds.
table_name = "ubereats_databricks1.silver.silver_payments"
quality_table = "ubereats_databricks1.silver.pipeline_quality_results"
quality_record = [{
    "run_id": str(__import__("uuid").uuid4()),
    "dataset_name": "payments",
    "source_rows": int(df_payments_table_row_count),
    "prepared_rows": int(df_payments_table.count()),
    "target_rows_after": int(spark.table(table_name).count()),
    "status": "SUCCEEDED",
    "recorded_at_utc": str(spark.sql("SELECT current_timestamp() AS ts").first()["ts"]),
}]
spark.createDataFrame(quality_record).write.format("delta").mode("append").saveAsTable(quality_table)
print("payments.quality.persisted_run_id=" + quality_record[0]["run_id"])
