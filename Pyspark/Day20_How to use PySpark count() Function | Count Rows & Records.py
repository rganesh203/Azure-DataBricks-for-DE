# Databricks notebook source
# Sample data
data = [
    (1, "Alice", 29),
    (2, "Bob", 35),
    (3, "Charlie", 40),
    (4, "David", 23),
    (5, "Eva", 31)
]
# Define schema (columns)
columns = ["id", "name", "age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.display()

# COMMAND ----------

total_rows = df.count()
print("Total number of rows:", total_rows)

# COMMAND ----------

# Count how many people are older than 30
count_age_30 = df.filter(df.age > 30).count()
print("Rows where age > 30:", count_age_30)
