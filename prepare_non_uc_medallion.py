"""Get lakehouse.medallion ready for a pipeline run: create it, or clear the last run.
 
SDP can't replace a table that already exists in Unity Catalog, so a rerun of the
pipeline starts by dropping the tables the previous run created.
"""
 
from common import spark
from pyspark.sql.types import StructType
import json

# Reconstruct the PySpark schema from the JSON string
with open("non_uc_pipeline/bronze_orders_schema.json", 'r') as f:
    schema_json = json.load(f)

schema = StructType.fromJson(schema_json)

session = spark()

df = session.createDataFrame([], schema)
df.show()

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("s3://lakehouse/tables/bronze/orders")


with open("non_uc_pipeline/bronze_locations_schema.json", 'r') as f:
    schema_json = json.load(f)

schema = StructType.fromJson(schema_json)

session = spark()

df = session.createDataFrame([], schema)
df.show()

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("s3://lakehouse/tables/bronze/locations")

# Silver
with open("non_uc_pipeline/silver_orders_schema.json", 'r') as f:
    schema_json = json.load(f)

schema = StructType.fromJson(schema_json)

session = spark()

df = session.createDataFrame([], schema)
df.show()

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("s3://lakehouse/tables/silver/orders")


# Silver
with open("non_uc_pipeline/gold_revenue_schema.json", 'r') as f:
    schema_json = json.load(f)

schema = StructType.fromJson(schema_json)

session = spark()

df = session.createDataFrame([], schema)
df.show()

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("s3://lakehouse/tables/gold/revenue_by_brand")

