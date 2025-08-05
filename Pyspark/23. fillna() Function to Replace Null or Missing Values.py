# Databricks notebook source
# DBTITLE 1,Sample DataFrame with Nulls
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Sample data with null values
data = [
    (1, "Alice", 25),
    (2, None, 30),
    (3, "Charlie", None),
    (4, None, None)
]

# Define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------

# DBTITLE 1,Replace All Nulls with a Specific Value
df_filled = df.fillna("Unknown")
df_filled.display()


# COMMAND ----------

# DBTITLE 1,Replace Nulls Column-wise
df_filled_cols = df.fillna({"name": "Unknown", "age": 0})
df_filled_cols.display()

