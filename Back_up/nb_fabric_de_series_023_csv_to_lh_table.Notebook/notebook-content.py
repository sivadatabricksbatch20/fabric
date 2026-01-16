# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_de_series_023_csv_to_lh_table
# 
# New notebook

# In[2]:


df_movies_1 = spark.read.format("csv").load("Files/fabric_de_series_023/movies_1.csv")
display(df_movies_1)


# In[5]:


df_movies_1 = spark.read.format("csv").option("header","true").load("Files/fabric_de_series_023/movies_1.csv")
display(df_movies_1)
df_movies_1.write.mode("overwrite").format("delta").saveAsTable("fabric_de_series_023.movies_1")


# In[6]:


df_movies_2 = spark.read.format("csv").load("Files/fabric_de_series_023/movies_2.csv")
display(df_movies_2)


# In[7]:


df_movies_2 = spark.read.format("csv").option("delimiter", ";").load("Files/fabric_de_series_023/movies_2.csv")
display(df_movies_2)


# In[9]:


from pyspark.sql.types import *
import json

movies_2_schema = '''
{
	"fields": [
		{
			"metadata": {},
			"name": "FILM",
			"nullable": true,
			"type": "string"
		},
		{
			"metadata": {},
			"name": "YEAR",
			"nullable": true,
			"type": "string"
		},
		{
			"metadata": {},
			"name": "RATING",
			"nullable": true,
			"type": "string"
		}
	],
	"type": "struct"
}
'''
movies_2_schema_st = StructType.fromJson(json.loads(movies_2_schema))
print(movies_2_schema_st)
df_movies_2 = spark.read.format("csv").option("delimiter", ";").schema(movies_2_schema_st).load("Files/fabric_de_series_023/movies_2.csv")
display(df_movies_2)
df_movies_2.write.mode("overwrite").format("delta").saveAsTable("fabric_de_series_023.movies_2")


# In[10]:


df_movies_2.schema.json()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
