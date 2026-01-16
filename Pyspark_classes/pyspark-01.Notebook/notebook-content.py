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

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [(243, 'siva' , 5000),(247, 'suman' , 5000),(258, 'gopi' , 5000), (243, 'siva' , 5000),
(247, 'suman' , 5000),(258, 'gopi' , 5000),(243, 'siva' , 5000),
(247, 'suman' , 5000),(258, 'gopi' , 5000),]
sch = ['eno', 'ename', 'esal'] 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.createDataFrame(data, sch)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Files/Data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format('csv').option('header', 'true').save('Files/Data/employee')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Files/Data/employee

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1 = spark.read.format('csv').option('header', 'true').load('Files/Data/employee')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1.write.format('csv').option('header', 'true').save('Files/Data1/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.columns

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

df_1.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_2 = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('Files/Data/employee')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_2.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

StructType()----data , columns
StructField()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

StructField('eno', IntegerType(), True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Sc1 = StructType([StructField('eno', IntegerType(), True) ,
                  StructField('ename', StringType(), True) ,
                  StructField('esal', IntegerType(), True)     ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_3 = spark.read.format('csv').option('header', 'true').schema(Sc1).load('Files/Data/employee')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_3.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Eno = 'siva'   --IntegerType
Eno = 'siva'   --- StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [(243, 'siva' ,'testing'),(247, 'suman' , 5000),(258, 'gopi' , 5000), (207, 'eswar' , 15000)]
sch = ['eno', 'ename', 'esal'] 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_4 = spark.createDataFrame(data,sch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_4.write.format('csv').option('header','true').save('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Sc1 = StructType([StructField('eno', IntegerType(), True) ,
                  StructField('ename', StringType(), True) ,
                  StructField('esal', IntegerType(), True)     ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_5 = spark.read.format('csv').schema(Sc1).option('header', 'true').load('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_5.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_5.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_6 = spark.read.format('csv').option('header', 'true').load('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_6.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_6.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

1) let me know the records
2) ignore those records
3) stop the process

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_7 = spark.read.format('csv').schema(Sc1).option('header', 'true').option('mode', 'dropMalFormed').load('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_7)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_8 = spark.read.format('csv').schema(Sc1).option('header', 'true').option('mode', 'PERMISSIVE').load('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_8.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_9 = spark.read.format('csv').schema(Sc1).option('header', 'true').option('mode', 'FAILFAST').load('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_9.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Sc2 = StructType([StructField('eno', IntegerType(), True) ,
                  StructField('ename', StringType(), True) ,
                  StructField('esal', IntegerType(), True),
                    StructField('badrecord', StringType(), True)   ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_9 = spark.read.format('csv').schema(Sc2).option('header', 'true').option('mode', 'permissive').option('columnNameofCorruptRecord', 'badrecord').load('Files/Data2/emp')






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_9.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4 = df_4.filter(col('eno') == 243)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4.write.format('csv').option('header','true').save('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.format('csv').option('header', 'true').load('Files/Data2/emp').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4.write.format('csv').mode('error').option('header','true').save('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4.write.format('csv').mode('append').option('header','true').save('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df4.write.format('csv').mode('overwrite').option('header','true').save('Files/Data2/emp')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
