# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# | Function           | Null Array | Empty Array | Index Returned? |
# | ------------------ | ---------- | ----------- | --------------- |
# | `explode`          | ❌ removed  | ❌ removed   | ❌ no            |
# | `explode_outer`    | ✔ kept     | ✔ kept      | ❌ no            |
# | `posexplode`       | ❌ removed  | ❌ removed   | ✔ yes           |
# | `posexplode_outer` | ✔ kept     | ✔ kept      | ✔ yes           |


# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
    (1, ["apple", "banana", "mango"]),
    (2, ["orange",'test']),
    (3, None),
    (4, [])
]

schema = ["id", "fruits"]

df = spark.createDataFrame(data, schema)
df.show(truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



df.select("id", explode("fruits").alias("fruit")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



df.select("id", explode_outer("fruits").alias("fruit")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import posexplode

df.select("id", posexplode("fruits")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import posexplode_outer

df.select("id", posexplode_outer("fruits")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.createDataFrame([
    (1, [10, 20, 30, 40]),
    (2, [100, 200]),
    (3, None),
    (4, [])
], ["id", "nums"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2.select("id", size("nums").alias("len")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2.select("id", size("nums").alias("len")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2.select("id", array_contains("nums", 20).alias("has_20")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2.select("id", sort_array("nums").alias("sorted")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2.select("id", array_join("nums", "-").alias("joined")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2.select("id", array_distinct("nums")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2.select("id", array_position("nums", 20)).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3 = spark.createDataFrame([(1, [[1,2], [3,4]])], ["id","arr"])
df3.select(flatten("arr")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4 = spark.createDataFrame([
    (1, ["a","b"], ["b","c"]),
    (2, ["x"], ["y"])
], ["id","a1","a2"])

df4.select("id", arrays_overlap("a1","a2")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
