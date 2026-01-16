-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {}
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!

CREATE TABLE IF NOT EXISTS env_timestamps (
    env STRING,
    t1 TIMESTAMP
)
USING delta;



-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
