"""Read a gold table by name, and list what the pipeline put in the catalog."""
 
from common import spark, uc
 
spark().sql("""
    SELECT * FROM lakehouse.medallion.revenue_by_brand ORDER BY revenue DESC
""").show()
 
for table in uc("GET", "/tables?catalog_name=lakehouse&schema_name=medallion")["tables"]:
    print(table["name"], table["table_type"], table["data_source_format"])