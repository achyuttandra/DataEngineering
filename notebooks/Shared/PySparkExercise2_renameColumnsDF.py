# Databricks notebook source
data = [(1001,"Chicago",535),(1002,"Boston",495),(1003,"Seattle",318)]

# COMMAND ----------

columns = ["station_id","city","rainfall"]

# COMMAND ----------

df = spark.createDataFrame(data = data,schema = columns)

# COMMAND ----------

df.show(n=5,truncate = False)

# COMMAND ----------

df.display()

# COMMAND ----------

df = spark.createDataFrame([(1001,"Chicago",535),(1002,"Boston",495),(1003,"Seattle",318)],["station_id","city","rainfall"])

# COMMAND ----------

df.show(5)


# COMMAND ----------

#Print Schema
df.printSchema()

# COMMAND ----------

#to change the dataframe to pandas
df.limit(5).toPandas()

# COMMAND ----------

df_2 = df.withColumnRenamed("city","city_Us")

# COMMAND ----------

df_2.show(5)

# COMMAND ----------

# Casting column types
from pyspark.sql.types import StringType
df_3 = df.withColumn("station_id",df["station_id"].cast(StringType()))

# COMMAND ----------

df_4 = df.selectExpr("cast(station_id as long)","cast(rainfall as float)")

# COMMAND ----------

df_5 = df.selectExpr("station_id","city")

# COMMAND ----------

df_5.show()

# COMMAND ----------

