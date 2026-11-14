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

def create_bucket(bucket):
    client = s3()
    existing = [b["Name"] for b in client.list_buckets()["Buckets"]]
    if bucket not in existing:
        client.create_bucket(Bucket=bucket)
    return client
 
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