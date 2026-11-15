"""Read a gold table by name, and list what the pipeline put in the catalog."""
 
from common import spark, uc_list_tables

print("=========== BRONZE TABLES ==========")
spark().sql("""
    SELECT event_date, count(*) AS events
    FROM lakehouse.tutorial.orders_raw
    GROUP BY event_date ORDER BY event_date
""").show()

spark().sql("""
    SELECT event_date, count(*) AS events
    FROM lakehouse.medallion.orders_bronze
    GROUP BY event_date ORDER BY event_date
""").show()

print("=========== GOLD TABLES ==========")

spark().sql("""
    SELECT * FROM lakehouse.medallion.revenue_by_brand ORDER BY revenue DESC
""").show()

uc_list_tables(catalog_name="lakehouse", schema_name="medallion")
