"""Create the `lakehouse` catalog, register orders_raw in it, and query it by name."""
 
from common import spark, uc
 
if "lakehouse" not in [c["name"] for c in uc("GET", "/catalogs")["catalogs"]]:
    uc("POST", "/catalogs", {"name": "lakehouse", "storage_root": "s3://lakehouse/managed"})
 
session = spark()
session.sql("CREATE SCHEMA IF NOT EXISTS lakehouse.tutorial")
session.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.tutorial.orders_raw
    USING delta LOCATION 's3://lakehouse/tables/orders_raw'
""")
session.sql("""
    SELECT event_date, count(*) AS events
    FROM lakehouse.tutorial.orders_raw
    GROUP BY event_date ORDER BY event_date
""").show()
 
# Ask the catalog itself, over its REST API.
for table in uc("GET", "/tables?catalog_name=lakehouse&schema_name=tutorial")["tables"]:
    print(table["name"], table["table_type"], table["data_source_format"], table["storage_location"])