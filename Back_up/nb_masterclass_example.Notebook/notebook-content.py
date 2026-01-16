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

# ## nb_masterclass_example
# 
# New notebook

# In[2]:


# Create sample data
data = [
    (1, "Alice", 29, "New York"),
    (2, "Bob", 35, "Los Angeles"),
    (3, "Cathy", 24, "Chicago"),
    (4, "David", 42, "San Francisco"),
    (5, "Eva", 31, "Seattle")
]

# Define schema (column names)
columns = ["ID", "Name", "Age", "City"]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)
display(df)


# # Markdown example
# This is just and example!

# In[3]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# SELECT 1 AS A


# In[4]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sparkr
# r_var <- "this is a R variable!"
# print(r_var)


# In[1]:


df = spark.sql("SELECT * FROM lh_fabric_de_series_02.fabric_de_series_022.animals LIMIT 1000")
display(df)


# In[2]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install pandas


# In[3]:


get_ipython().run_cell_magic('sh', '', 'ls\n')


# In[4]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %run nb_masterclass_example_2


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
