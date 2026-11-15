"""Turn on UniForm: orders_raw stays a Delta table and also gets Iceberg metadata."""
 
from common import s3, spark
 
spark().sql("""
    ALTER TABLE lakehouse.tutorial.orders_raw SET TBLPROPERTIES (
        'delta.universalFormat.enabledFormats' = 'iceberg',
        'delta.enableIcebergCompatV2' = 'true',
        'delta.columnMapping.mode' = 'name'
    )
""")
 
for obj in s3().list_objects_v2(Bucket="lakehouse", Prefix="tables/orders_raw/metadata/")["Contents"]:
    print(obj["Key"])