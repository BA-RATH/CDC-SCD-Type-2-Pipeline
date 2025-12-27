spark.sql("""
CREATE TABLE IF NOT EXISTS cdc_cust_bronze
USING DELTA
LOCATION 'dbfs:/FileStore/barath/tables/cdc_cust_bronze'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS cdc_cust_silver (
  customer_id STRING,
  name STRING,
  email STRING,
  address STRING,
  row_hash STRING,
  is_current BOOLEAN,
  effective_from TIMESTAMP,
  effective_to TIMESTAMP
)
USING DELTA
LOCATION 'dbfs:/FileStore/barath/tables/cdc_cust_silver'
""")
