# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d9ebad35-6987-4924-8dbf-677847936bf0",
# META       "default_lakehouse_name": "first_lakehoue",
# META       "default_lakehouse_workspace_id": "9ec5a999-67d7-41a8-8979-6c8ebda21878",
# META       "known_lakehouses": [
# META         {
# META           "id": "d9ebad35-6987-4924-8dbf-677847936bf0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark-submit \
  --master spark://driver-fabric:7077 \
  --deploy-mode client \
  --name "Fabric Notebook Job" \
  --class org.apache.spark.deploy.SparkSubmit \
  --conf spark.fabric.workspace.id=<workspace-guid> \
  --conf spark.fabric.item.id=<lakehouse-guid> \
  --conf spark.fabric.capacity.sku=F4 \
  --conf spark.fabric.onelake.root=/onelake/<workspace-guid>/ \
  --conf spark.hadoop.fs.onelake.impl=com.microsoft.fabric.onelake.OneLakeFileSystem \
  --conf spark.hadoop.fs.onelake.account=<workspace-guid> \
  --conf spark.databricks.service.enabled=false \
  --conf spark.fabric.identity=<aad-user-guid> \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.delta.logStore.class=com.microsoft.fabric.delta.FabricLogStore \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.fabric.photon.enabled=true \
  --conf spark.fabric.autoscale.minExecutors=2 \
  --conf spark.fabric.autoscale.maxExecutors=12 \
  --driver-memory 8G \
  --driver-cores 4 \
  --executor-memory 16G \
  --executor-cores 4 \
  --num-executors 8 \
  /fabric/spark/python/pyspark-shell.py \
  /onelake/workspaces/<id>/notebooks/generated_<run-id>.py \
  --notebook-parameters <json>


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark-submit \
  --master spark://<driver-ip>:7077 \
  --deploy-mode client \
  --name "Databricks Notebook Job" \
  --class org.apache.spark.deploy.SparkSubmit \
  --conf spark.databricks.clusterUsageTags.autoTerminationMinutes=60 \
  --conf spark.databricks.clusterUsageTags.clusterAllTags=[...] \
  --conf spark.databricks.clusterUsageTags.clusterName=cluster-01 \
  --conf spark.databricks.repl.allowedLanguages=python,sql,r,scala \
  --conf spark.databricks.repl.extraClassPath=/databricks/jars/* \
  --conf spark.driver.extraClassPath=/databricks/jars/* \
  --conf spark.executor.extraClassPath=/databricks/jars/* \
  --conf spark.databricks.service.client.enabled=true \
  --conf spark.databricks.service.address=https://adb-123456.2.azuredatabricks.net \
  --conf spark.databricks.api.token=<internal-job-token> \
  --conf spark.databricks.api.url=https://adb-123456.2.azuredatabricks.net \
  --conf spark.databricks.delta.preview.enabled=true \
  --conf spark.sql.extensions=com.databricks.sql.transaction.tahoe.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.AbstractFileSystem.dbfs.impl=com.databricks.dbfs.DBFS \
  --conf spark.hadoop.fs.dbfs.impl=com.databricks.dbfs.DBFSFileSystem \
  --conf spark.databricks.io.cache.enabled=true \
  --conf spark.databricks.tahoe.optimizeWrite.enabled=true \
  --conf spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite=true \
  --conf spark.databricks.delta.properties.defaults.autoOptimize.autoCompact=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrator=com.databricks.spark.util.DatabricksKryoRegistrator \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.broadcastTimeout=36000 \
  --conf spark.network.timeout=800s \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  --conf spark.databricks.cluster.profile=serverless \
  --conf spark.databricks.driver.disableScalaOutput=true \
  --conf spark.databricks.repl.localWriteDir=/local_disk0/tmp \
  --conf spark.databricks.clusterUsageTags.sparkVersion=13.3.x-scala2.12 \
  --conf spark.databricks.clusterUsageTags.platform=azure \
  --conf spark.sql.execution.arrow.enabled=true \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.databricks.pyspark.enableProcessIsolation=false \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.databricks.delta.retentionDurationCheck.enabled=false \
  --conf spark.databricks.ucChain.enabled=true \
  --conf spark.databricks.pyspark.virtualenv.enabled=false \
  --driver-memory 16G \
  --driver-cores 4 \
  --executor-memory 24G \
  --executor-cores 8 \
  --num-executors 4 \
  /databricks/spark/python/pyspark-shell-main.py \
  /dbfs/databricks/scripts/generated_<run-id>.py \
  --notebook-params '{"param1": "value1"}'



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# | Feature    | Lineage                      | DAG                          |
# | ---------- | ---------------------------- | ---------------------------- |
# | Meaning    | History of transformations   | Execution plan               |
# | Level      | DataFrame / RDD level        | Job/Stage/Task level         |
# | Purpose    | Maintain fault tolerance     | Schedule tasks & parallelism |
# | When built | During transformations       | During an action             |
# | Direction  | Logical                      | Physical execution           |
# | Example    | `read → filter → join → agg` | Stage 1, Stage 2, Stage 3    |


# MARKDOWN ********************

# Performance related

# MARKDOWN ********************

# ❓What is lineage?
# 
# Lineage is the sequence of transformations that created a DataFrame. Spark uses lineage for fault tolerance.
# 
# ❓What is DAG?
# 
# DAG is the physical execution plan created when an action runs. It splits the job into stages and tasks.
# 
# ❓Difference?
# 
# Lineage = Logical plan
# DAG = Execution plan
# 
# ❓How to view lineage?
# 
# df.explain(True) or Spark UI → SQL Query Plan
# 
# ❓How to view DAG?
# 
# Spark UI → DAG Visualization

# MARKDOWN ********************

# Your Code (SQL/DataFrame)
#         │
#         ▼
# 1. Unresolved Logical Plan
#         │
#         ▼
# 2. Resolved Logical Plan
#         │
#         ▼
# 3. Optimized Logical Plan  ← Catalyst Rules Applied
#         │
#         ▼
# 4. Physical Plan (Stages + Tasks)
#         │
#         ▼
#  Spark Execution (Executors)


# MARKDOWN ********************

# ⭐ Spark Internal Join Types (Execution Strategies)
# 
# Inside Spark, joins are executed using 3 major strategies:
# 
# 1️⃣ Broadcast Hash Join (BHJ)
# 2️⃣ Shuffle Hash Join (SHJ)
# 3️⃣ Sort-Merge Join (SMJ)


# MARKDOWN ********************

# ⭐ 1. Broadcast Hash Join (BHJ)
# 
# 👉 Spark broadcasts the SMALL table to all executors.
# 👉 Then joins happen locally without shuffle.
# 👉 FASTEST join in Spark.
# 
# Internal Conditions
# 
# One table is small (< ~500 MB)
# 
# Or forced using broadcast() hint
# 
# Internal Process
# 
#   Driver → collect small table → broadcast to all executors  
#   Executors → hash build → probe on big table partitions  


# MARKDOWN ********************

# ⭐ 2. Shuffle Hash Join (SHJ)
# 
# 👉 Both tables are hashed on join keys
# 👉 Spark shuffles partitions
# 👉 Hash tables are created and probed
# 👉 Good for medium-sized tables
# 
# Internal Process
# 
# Repartition both tables on join key → shuffle
# Build hash table on one side → probe other side
# 
# Triggered When:
# 
# No table is small enough for broadcast
# 
# Join keys are not sorted
# 
# Cluster has memory to build hash tables

# MARKDOWN ********************

# ⭐ 3. Sort-Merge Join (SMJ)
# 
# 👉 Most common join for big data
# 👉 Used when both tables are VERY LARGE
# 👉 Requires sorting on join key + merge phase
# 👉 Heavy shuffle
# 
# Internal Process
# Shuffle both tables on join key  
# Sort partitions  
# Merge sorted streams  
# 
# Conditions for SMJ
# 
# Tables are big and unsorted
# 
# Broadcast disabled
# 
# Spark chooses SMJ by default when SHJ cannot run
# 
# Good for equality joins & multi-column joins


# MARKDOWN ********************

# Join Decision Flow (Simplified)
# 
# IF small table < broadcast threshold:
# 
#     → Broadcast Hash Join
# 
# ELSE IF hash join possible (enough memory):
# 
#     → Shuffle Hash Join
# 
# ELSE
# 
#     → Sort Merge Join


# MARKDOWN ********************

# ⭐ Memory Behavior of Internal Joins
# 
# | Join Type           | Shuffle?    | Memory Usage                | Best For      |
# | ------------------- | ----------- | --------------------------- | ------------- |
# | Broadcast Hash Join | ❌ none      | Small dimension fits in RAM | Star schemas  |
# | Shuffle Hash Join   | ✔ yes       | Medium memory               | Medium tables |
# | Sort Merge Join     | ✔ heavy     | Less memory, more CPU       | Big tables    |
# | Cartesian Join      | ✔ explosion | Very high                   | Rare cases    |


# CELL ********************

from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Create 2 DataFrames
df_sales = spark.createDataFrame(
    [(1, "A", 100), (2, "B", 150), (3, "A", 200), (4, "C", 300)],
    ["txn_id", "cust", "amount"]
)

df_cust = spark.createDataFrame(
    [("A", "North"), ("B", "South"), ("C", "East")],
    ["cust", "region"]
)


# 2. Wide Transformation 1: JOIN  → Causes Shuffle
df_join = df_sales.join(broadcast(df_cust), "cust")


# 3. Wide Transformation 2: GROUP BY → Causes Shuffle
df_group = df_join.groupBy("region").sum("amount")


# 4. Wide Transformation 3: ORDER BY → Causes Shuffle
df_final = df_group.orderBy("sum(amount)")


df_final.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.get("spark.sql.join.preferSortMergeJoin")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.get()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC set spark.sql.join.preferSortMergeJoin = false


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

broadcast or shuffle hash join ---AdaptiveSparkPlan

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.get("spark.sql.adaptive.enabled")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.adaptive.enabled", "true")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.explain()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.explain('extended')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5. Trigger an ACTION → Creates JOB with multiple STAGES & TASKS
df_final.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Create 2 DataFrames
df_sales = spark.createDataFrame(
    [(1, "A", 100), (2, "B", 150), (3, "A", 200), (4, "C", 300)],
    ["txn_id", "cust", "amount"]
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales.explain('extended')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sales.rdd.getNumPartitions()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sparkContext.defaultParallelism


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format('csv').load('Files/Data/employee')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.rdd.getNumPartitions()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.rdd.map(lambda x: len(str(x))).sum()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
