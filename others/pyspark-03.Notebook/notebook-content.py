# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

data_emp = [
    (1, "Alice", 5000, "HR"),
    (2, "Bob", 6000, "IT"),
    (3, "Charlie", 7000, "IT"),
    (4, "David", 5500, "HR"),
    (5, "David", 5500, "EEE")
]

data_dept = [
    ("HR", "Human Resources"),
    ("IT", "Information Technology"),
    ("FIN", "Finance")
]

emp_cols = ["emp_id", "name", "salary", "dept_id"]
dept_cols = ["dept_id", "dept_name"]

emp_df = spark.createDataFrame(data_emp, emp_cols)
dept_df = spark.createDataFrame(data_dept, dept_cols)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select *from emp

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.createOrReplaceTempView("emp")
dept_df.createOrReplaceTempView("dept")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df one joins( df two , condition, typeof join)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

joined_df = emp_df.join(dept_df,emp_df.dept_id == dept_df.dept_id, 'inner')
joined_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT e.emp_id, e.name, d.dept_name
# MAGIC FROM emp e
# MAGIC INNER JOIN dept d
# MAGIC ON e.dept_id = d.dept_id;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.join(dept_df, "dept_id", "inner") \
      .select("emp_id", "name", "dept_name") \
      .show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT e.emp_id, e.name, d.dept_name
# MAGIC FROM emp e
# MAGIC LEFT JOIN dept d
# MAGIC ON e.dept_id = d.dept_id;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.join(dept_df, "dept_id", "left").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT e.emp_id, e.name, d.dept_name
# MAGIC FROM emp e
# MAGIC RIGHT JOIN dept d
# MAGIC ON e.dept_id = d.dept_id;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.join(dept_df, "dept_id", "right").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *
# MAGIC FROM emp e
# MAGIC FULL OUTER JOIN dept d
# MAGIC ON e.dept_id = d.dept_id;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.join(dept_df, "dept_id", "outer").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dept_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.join(dept_df, "dept_id", "left").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT e.*
# MAGIC FROM emp e
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM dept d
# MAGIC     WHERE e.dept_id = d.dept_id
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.join(dept_df, "dept_id", "left_semi").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT e.*
# MAGIC FROM emp e
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM dept d
# MAGIC     WHERE e.dept_id = d.dept_id
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.join(dept_df, "dept_id", "left_anti").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
