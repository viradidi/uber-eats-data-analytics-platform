# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types  import *
import sys
sys.path.append("/Workspace/Users/sivana9908_gmail.com#ext#@sivana9908gmail.onmicrosoft.com/Uber-Eats-End-to-End-_Azure-Data-Engineering-Project")
from src.common.spark_utils import standardize_columns
from delta.tables import DeltaTable

# COMMAND ----------

#Reading data from adls 
df_drivers = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("abfss://bronze@ubereaststorage.dfs.core.windows.net/csv/drivers/")
)
display(df_drivers)
df_drivers.printSchema()

# COMMAND ----------

# standardize the columns
df_drivers=standardize_columns(df_drivers)

# standardize the data
df_drivers = df_drivers.withColumn("vehicle_type",upper(trim((col("vehicle_type")))))\
                       .withColumn("status",upper(trim((col("status")))))\
                       .withColumn("city",initcap(trim((col("city")))))

#handle the null cases

df_drivers= df_drivers.filter(col("driver_id").isNotNull())
                       
#deduplication 
df_drivers = df_drivers.drop_duplicates(["driver_id"])

#applying business rules
df_drivers = df_drivers.filter(col("vehicle_type").isin("SCOOTER","BIKE"))

#VALIDATION
buinesskey_dupliates =df_drivers.groupBy("driver_id").count().filter(col("count")>1).count()
print("businesskey nulls",buinesskey_dupliates)

joindate_invalid = df_drivers.filter(col("join_date").isNull()).count()
print("invalidjoindates",joindate_invalid)







# COMMAND ----------

table_name = "ubereats_databricks1.silver.drivers"

if not spark.catalog.tableExists(table_name):

    df_drivers.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

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