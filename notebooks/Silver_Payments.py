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