# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "29ddef12-09d4-4dc3-a7b9-b4822e7df5ad",
# META       "default_lakehouse_name": "lake_for_copy",
# META       "default_lakehouse_workspace_id": "9ec5a999-67d7-41a8-8979-6c8ebda21878",
# META       "known_lakehouses": [
# META         {
# META           "id": "29ddef12-09d4-4dc3-a7b9-b4822e7df5ad"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.window import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# | Category    | Window Functions                              | Purpose                                        |
# | ----------- | --------------------------------------------- | ---------------------------------------------- |
# | Ranking     | row_number, rank, dense_rank, ntile           | Sort & rank rows                               |
# | Analytical  | lead, lag, first_value, last_value, nth_value | Compare current with previous/next rows        |
# | Aggregation | sum, avg, min, max, count over window         | Running totals, moving avg, cumulative metrics |


# CELL ********************

data = [
    ("A", "2025-01-01", 500),
    ("A", "2025-01-05", 300),
    ("A", "2025-01-10", 700),
    ("B", "2025-01-02", 400),
    ("B", "2025-01-05", 600),
    ("C", "2025-01-03", 900),
    ("C", "2025-01-08", 200)
]

schema = ["category", "txn_date", "amount"]

df = spark.createDataFrame(data, schema)
df.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView('emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *, row_number() over(partition by category order by amount) r1 from emp 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *, row_number() over(order by amount) r1 from emp 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(row_number().over(Window.orderBy(col('amount').desc())).alias('r1')).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select('*', row_number().over(Window.partitionBy('category').orderBy(col('amount').desc())).alias('r1')).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import *

w = Window.partitionBy("category").orderBy("txn_date")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select(row_number().over(w).alias('r1')).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("row_num", row_number().over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("rank", rank().over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("dense_rank", dense_rank().over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("percent_rank", percent_rank().over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("ntile_2", ntile(2).over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *, lead(amount,1,0) over(partition by category order by amount) l1 from emp

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("next_amount", lead("amount", 1,0).over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("prev_amount", lag("amount", 1).over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("first_amt", first_value("amount").over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w2 = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df.withColumn("last_amt", last_value("amount").over(w2)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("second_value", nth_value("amount", 2).over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("running_total", sum("amount").over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w_roll = w.rowsBetween(-1, 0)

df.withColumn("moving_avg", avg("amount").over(w_roll)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("cum_min", min("amount").over(w)).show()
df.withColumn("cum_max", max("amount").over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("running_count", count("amount").over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.withColumn("diff_prev", col("amount") - lag("amount",1).over(w)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Percentage Change**

# CELL ********************

df.withColumn(
    "pct_change",
    (col("amount") - lag("amount",1).over(w)) / lag("amount",1).over(w)
).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
