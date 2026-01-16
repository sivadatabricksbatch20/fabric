# Fabric notebook source


# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## nb_dp700_e009_notebook_example
# 
# New notebook

# # DP-700 Examp Prep Episode 009: Notebooks

# ## 🔹 Parameters

# In[11]:


dataset = "movies"


# ## 🔹 Reading data from a file in Lakehouse

# In[12]:


df = spark.read.format("csv").option("header","true").load(f"Files/dp700_e009/{dataset}.csv")


# ## 🔹 Displaying data

# In[13]:


display(df)


# ## 🔹 Using Spark SQL

# In[14]:


df.createOrReplaceTempView("df_view")


# In[15]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT
# *
# ,now() AS ts
# FROM df_view


# In[16]:


df_ts = spark.sql('''
    SELECT
    *
    ,now() AS ts
    FROM df_view
''')

display(df_ts)


# ## 🔹 Writing data to a table in Lakehouse

# In[17]:


spark.sql('''
CREATE SCHEMA IF NOT EXISTS dp700_e009
''')

df_ts.write.mode("overwrite").format("delta").saveAsTable(f"dp700_e009.{dataset}")


# ## 🔹 Reading data from a table in Lakehouse

# In[18]:


df_table = spark.sql(f"SELECT * FROM lh_dp700.dp700_e009.{dataset}")
display(df_table)


# ## 🔹 Using NotebooUtils to set an exit value for the notebook

# In[19]:


notebookutils.notebook.exit(f"Dataset {dataset} was processed ok!")

