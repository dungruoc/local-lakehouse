"""Get lakehouse.medallion ready for a pipeline run: create it, or clear the last run.
 
SDP can't replace a table that already exists in Unity Catalog, so a rerun of the
pipeline starts by dropping the tables the previous run created.
"""
 
from common import spark
 
session = spark()
session.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.medallion")
for row in session.sql("SHOW TABLES IN lakehouse.medallion").collect():
    session.sql(f"DROP TABLE lakehouse.medallion.{row.tableName}")
    print(f"dropped lakehouse.medallion.{row.tableName}")
print("lakehouse.medallion is ready")