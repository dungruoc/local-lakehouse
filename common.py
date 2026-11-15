"""Endpoints and clients that every script in this tutorial shares."""
 
import json
import os
import urllib.request
 
import boto3
from pyspark.sql import SparkSession
 
SPARK_REMOTE = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:8333")
UC_URL = os.environ.get("UC_URL", "http://localhost:8081")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5001")
S3_KEY, S3_SECRET = "lakehouse", "lakehouse-secret"
 
 
def spark():
    """A Spark Connect session: a thin client, the work runs in the cluster."""
    return SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
 
 
def s3():
    """An S3 client for SeaweedFS."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
    )


def s3_list_bucket():
    client = s3()
    return (client.list_buckets()["Buckets"], client)

def s3tables():
    """An S3 client for SeaweedFS."""
    return boto3.client(
        "s3tables",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_KEY,
        aws_secret_access_key=S3_SECRET,
        region_name="us-east-1",
    )

def s3tables_list_buckets():
    client = s3tables()
    return (client.list_table_buckets()["tableBuckets"], client)

def s3_create_bucket(bucket):
    bk_list, client = s3_list_bucket()
    existing = [b["Name"] for b in bk_list]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
    return client

def s3_create_folder(bucket_name, folder_path):
    client = s3()
    client.put_object(Bucket=bucket_name,
                      Key=folder_path + '/')

def s3_iceberg_get_latest_metafile(bucket_name, table_location):
    # The table's current Iceberg metadata is the newest metadata.json UniForm wrote.
    listing = s3().list_objects_v2(Bucket=bucket_name, Prefix=f"{table_location}/metadata/")
    newest = max(
        (obj for obj in listing["Contents"] if obj["Key"].endswith(".metadata.json")),
        key=lambda obj: obj["LastModified"],
    )
    location = f"s3://{bucket_name}/{newest['Key']}"
    return location

def uc(method, path, body=None):
    """Call the Unity Catalog REST API and return the JSON reply."""
    request = urllib.request.Request(
        f"{UC_URL}/api/2.1/unity-catalog{path}",
        data=json.dumps(body).encode() if body else None,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request) as reply:
        return json.load(reply)

def uc_get_catalogs():
    return uc("GET", "/catalogs")["catalogs"]

def uc_create_catalog(catalog_name, storage_root):
    uc("POST", "/catalogs", {"name": catalog_name, "storage_root": storage_root})

def uc_list_tables(catalog_name, schema_name):
    # Ask the catalog itself, over its REST API.
    for table in uc("GET", f"/tables?catalog_name={catalog_name}&schema_name={schema_name}")["tables"]:
        print(table["name"], table["table_type"], table["data_source_format"], table["storage_location"])