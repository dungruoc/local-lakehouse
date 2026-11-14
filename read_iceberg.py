"""Read orders_raw as an Iceberg table with PyIceberg and DuckDB. No Spark involved."""
 
import duckdb
from common import S3_ENDPOINT, S3_KEY, S3_SECRET, s3
from pyiceberg.table import StaticTable
 
# The table's current Iceberg metadata is the newest metadata.json UniForm wrote.
listing = s3().list_objects_v2(Bucket="lakehouse", Prefix="tables/orders_raw/metadata/")
newest = max(
    (obj for obj in listing["Contents"] if obj["Key"].endswith(".metadata.json")),
    key=lambda obj: obj["LastModified"],
)
location = f"s3://lakehouse/{newest['Key']}"
print(f"Reading {location}\n")
 
table = StaticTable.from_metadata(location, properties={
    "s3.endpoint": S3_ENDPOINT,
    "s3.access-key-id": S3_KEY,
    "s3.secret-access-key": S3_SECRET,
    "s3.region": "us-east-1",
})
rows = table.scan(selected_fields=("event_id",)).to_arrow().num_rows
print(f"PyIceberg: {rows:,} rows (Iceberg v{table.format_version}, "
      f"converted from Delta version {table.properties['delta-version']})\n")
 
duck = duckdb.connect()
duck.sql("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
duck.sql(f"""
    CREATE SECRET (TYPE s3, KEY_ID '{S3_KEY}', SECRET '{S3_SECRET}', REGION 'us-east-1',
                   ENDPOINT '{S3_ENDPOINT.split("://")[1]}', URL_STYLE 'path', USE_SSL false)
""")
duck.sql(f"""
    SELECT event_date, count(*) AS events
    FROM iceberg_scan('{location}')
    GROUP BY event_date ORDER BY event_date
""").show()