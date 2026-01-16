# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, rand, floor
from pyspark import StorageLevel

sales_data = [
    (1, 101, "IN", 100),
    (2, 102, "IN", 200),
    (3, 103, "US", 300),
    (4, 101, "IN", 150),
    (5, 104, "US", 400),
    (6, 101, "IN", 500)
]

sales_df = spark.createDataFrame(
    sales_data,
    ["order_id", "customer_id", "country", "amount"]
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.cache()
sales_df.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_json(df):
    """
    Flattens a DataFrame with complex nested fields (Arrays and Structs) by
    converting them into individual columns.

    Parameters:
    - df: The input DataFrame with complex nested fields

    Returns:
    - The flattened DataFrame with all complex fields expanded into separate columns.
    """

    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([
        (field.name, field.dataType)
        for field in df.schema.fields
        if type(field.dataType) == ArrayType or type(field.dataType) == StructType
    ])
    print(df.schema)
    print("")

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns (flatten structs)
        if type(complex_fields[col_name]) == StructType:
            expanded = [
                col(f"{col_name}.{k}").alias(f"{col_name}_{k}")
                for k in [n.name for n in complex_fields[col_name]]
            ]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using explode (explode arrays)
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        # recompute remaining Complex Fields in Schema
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == ArrayType or type(field.dataType) == StructType
        ])

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
