# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types  import *
import sys
sys.path.append("/Workspace/Users/sivana9908_gmail.com#ext#@sivana9908gmail.onmicrosoft.com/Uber-Eats-End-to-End-_Azure-Data-Engineering-Project")
from src.common.spark_utils import standardize_columns
from delta.tables import DeltaTable

# COMMAND ----------

#Reading data from adls 
df_promotions= (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("abfss://bronze@ubereaststorage.dfs.core.windows.net/csv/promotions/")
)
display(df_promotions)
df_promotions.printSchema()

# COMMAND ----------

# standardize the columns
df_promotions=standardize_columns(df_promotions)


                       
#deduplication 
df_promotions = df_promotions.drop_duplicates(["promo_id","restaurant_id"]) 

#applying business rules
df_promotions = df_promotions.filter(col("discount_value")>0)

display(df_promotions)










# COMMAND ----------

table_name = "ubereats_databricks1.silver.promotions"

if not spark.catalog.tableExists(table_name):

    df_promotions.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

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