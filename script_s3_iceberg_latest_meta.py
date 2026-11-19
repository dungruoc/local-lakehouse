from common import s3_iceberg_get_latest_metafile, s3
import sys

bucket_name = sys.argv[1]
table_location = sys.argv[2]

# listing = s3().list_objects_v2(Bucket=bucket_name, Prefix=f"{table_location}/metadata/")

meta = s3_iceberg_get_latest_metafile(bucket_name, table_location)
print(meta)
