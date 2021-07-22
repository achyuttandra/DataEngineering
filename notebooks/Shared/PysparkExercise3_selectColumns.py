# Databricks notebook source
df = spark.createDataFrame([(1001,"Chicago",395),(1002,"New York",600),(1003,"Maine",500)],["station_id","city","rainfall"])

# COMMAND ----------

df.show(5)

# COMMAND ----------

df.limit(3).toPandas()

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Using cast to change datatype of column
from pyspark.sql.types import StringType
df_2 = df.withColumn("station_id",df["station_id"].cast(StringType()))

# COMMAND ----------

df_2["station_id"].display()

# COMMAND ----------

csv_file_path = 