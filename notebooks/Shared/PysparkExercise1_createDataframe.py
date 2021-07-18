# Databricks notebook source
#check the version of spark
spark.version

# COMMAND ----------

# defining a dataframe
df = spark.createDataFrame (
[
  (1001,"Chicago",535),
  (1002,"Boston",495),
  (1003,"Seattle",318)
],
["station_id","city","rainfall"])

# COMMAND ----------

#to see as sample of dataframe with number of records
df.show(n=5,truncate=False)


# COMMAND ----------

#to visualize dataframe
df.display()

# COMMAND ----------

#number of entries in the dataframe
df.count()

# COMMAND ----------

# defining a data list
data = [(1001,"Chicago",535),(1002,"Boston",495),(1003,"Chicago",318)]

# COMMAND ----------

# columns
columns = ["station_id","city","rainfall"]

# COMMAND ----------

df_1 = spark.createDataFrame(data = data,schema = columns)

# COMMAND ----------

