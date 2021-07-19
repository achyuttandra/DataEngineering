# Databricks notebook source
#import pandas and numpy libraries
import pandas as pd
import numpy as np

# COMMAND ----------

# create a new dataframe for the csv file
df = pd.read_csv("/dbfs//FileStore/tables/dcad_data/web_sales.csv")

# COMMAND ----------

df.head()

# COMMAND ----------

missing_values = ["--","n/a","","na"]

# COMMAND ----------

df = pd.read_csv("/dbfs//FileStore/tables/dcad_data/web_sales.csv", na_values = missing_values)

# COMMAND ----------

df

# COMMAND ----------

# to drop some columns 



to_drop = ["ws_item_sk"]
df.head()

#df.drop(to_drop,inplace=True)

# COMMAND ----------

df.head()

# COMMAND ----------

df.head()

df.tail()

df.info()

df.describe()

df.iloc[0]

df.loc([0],var)




# COMMAND ----------

df = (pd.melt(df)
.rename(columns={
'variable' : 'var',
'value' : 'val'})
.query('val >= 200')
)


# COMMAND ----------

df.head()

# COMMAND ----------

import pandas as pd
import numpy as np

# crete a sample dataframe
data = pd.DataFrame({
    'age' :     [ 10, 22, 13, 21, 12, 11, 17],
    'section' : [ 'A', 'B', 'C', 'B', 'B', 'A', 'A'],
    'city' :    [ 'Gurgaon', 'Delhi', 'Mumbai', 'Delhi', 'Mumbai', 'Delhi', 'Mumbai'],
    'gender' :  [ 'M', 'F', 'F', 'M', 'M', 'M', 'F'],
    'favourite_color' : [ 'red', np.NAN, 'yellow', np.NAN, 'black', 'green', 'red']
})

# view the data
data

# COMMAND ----------

# select all rows with a condition

data.loc[data.age>=15]

# COMMAND ----------

data.loc[(data.age >=12) & (data.gender == 'M')]


# COMMAND ----------

data.loc[1:3]

# COMMAND ----------

data.loc[(data.age>=15),['city','gender']]

# COMMAND ----------

data.loc[(data.age>=12),['section']] = 'M'
data

# COMMAND ----------

data.loc[(data.age>=17),['section','city']] = ['S','Pune']
data

# COMMAND ----------

data.iloc[1:2]

# COMMAND ----------

# to select a particular index and particular columns
data.iloc[[0,2],[1,3]]


# COMMAND ----------

