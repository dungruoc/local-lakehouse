# Resources

This repo is to reproduce the tutorial:
[TUTORIAL: How to Build an Open Lakehouse on Your Laptop](https://systemsnull.net/posts/data-engineering-ai/open-lakehouse-on-your-laptop)

But I add an extra section for putting Trino and its catalogs for query engine.

## Basic Requirements

- Memory and disk. The whole stack uses about **9 GB of RAM** once everything is up, so a 16 GB machine is comfortable; in Docker Desktop, raise the memory limit under `Settings > Resources` to at least 10 GB. Plan for about 12 GB of disk for images and downloads.
- Docker (with compose)
- Python (venv or conda)
- Rustc/Cargo + Bun (for Unity Catalog UI)

```bash
> conda create -n local-lakehouse python=3.14
> conda activate local-lakehouse
> pip install pyspark-client==4.2.0 pandas==2.3.3 boto3==1.43.93 kafka-python==2.3.0 pyiceberg==0.12.0 duckdb==1.5.6 mlflow-skinny==3.16.1
```

# Storage: SeaweedFS

## We create first a bucket for our lakehouse
```bash
·> python 01_create_lakehouse_bucket.py
hello.txt 17 bytes
```

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  st_PY["your Python"]:::new
  subgraph st_ST["storage"]
    st_SW[("SeaweedFS :8333<br/>bucket lakehouse")]:::new
  end
  st_PY -->|"S3 API"| st_SW
```

# Compute: Spark 4.2 and Spark Connect

Our lakehouse use Spark as compting engine which can read/write data to Seaweedfs

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  sp_PY["PySpark client"]:::new
  sp_SC["Spark Connect<br/>:15002"]:::new
  sp_CL["master + worker"]:::new
  sp_SW[("SeaweedFS")]:::old
  sp_PY -->|"gRPC"| sp_SC
  sp_SC --> sp_CL
  sp_CL -->|"S3A"| sp_SW
```


> We test a dummy Spark job
```bash
·> python 02_hello_spark.py
Connected to Spark 4.2.0 at sc://localhost:15002
+------------+
|       total|
+------------+
|499999500000|
+------------+
```


# Some data: a ghost-kitchen generator

This is a script to generate some data logs, writing to landing storage

```bash
·> python 03_generate_orders.py files
landing/orders/2026-09-01.parquet  4,720 rows
landing/orders/2026-09-02.parquet  4,382 rows
landing/orders/2026-09-03.parquet  5,479 rows
landing/orders/2026-09-04.parquet  4,508 rows
landing/orders/2026-09-05.parquet  5,194 rows
landing/orders/2026-09-06.parquet  4,962 rows
landing/orders/2026-09-07.parquet  4,776 rows
landing/locations/locations.parquet  4 rows
```

# Table format: Delta Lake
Now, we use spark to read data from landing storage and write into a delta table at location `TABLE_PATH = "s3://lakehouse/tables/orders_raw"`

```bash
·> python 04_load_orders.py
+-------+---------+-------------+
|version|operation|numOutputRows|
+-------+---------+-------------+
|      1|    WRITE|        29301|
|      0|    WRITE|         4720|
+-------+---------+-------------+

rows now: 34,021   rows at version 0: 4,720

tables/orders_raw/_delta_log/00000000000000000000.crc
tables/orders_raw/_delta_log/00000000000000000000.json
tables/orders_raw/_delta_log/00000000000000000001.crc
tables/orders_raw/_delta_log/00000000000000000001.json
tables/orders_raw/_delta_log/_staged_commits/
tables/orders_raw/part-00000-493168f5-9d04-4ce9-9dad-ee5a503af74f-c000.snappy.parquet
tables/orders_raw/part-00000-5ac9333a-1868-493b-8d9a-63c127d2995f-c000.snappy.parquet
tables/orders_raw/part-00001-6b619c3f-8a33-46d1-ae55-5d8e3a5882bf-c000.snappy.parquet
tables/orders_raw/part-00001-db641e30-998d-4c19-bbfa-dece791034ae-c000.snappy.parquet
```

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  dl_SC["Spark Connect"]:::old
  subgraph dl_SW["SeaweedFS"]
    dl_LOG["_delta_log<br/>a commit per version"]:::new
    dl_PQ["Parquet<br/>data files"]:::new
  end
  dl_SC -->|"commit"| dl_LOG
  dl_SC -->|"write"| dl_PQ
  dl_LOG -.->|"lists"| dl_PQ
```

# Catalog: Unity Catalog OSS

The delta table exist now in S3 storage, we can query it with delta location, but we can register it into Unity Catalog to ease the management.

The following script will
- create a catalog `lakehouse` at `s3://lakehouse/managed`
- create a schema `lakehouse.tutorial`
- register the delta table created in S3 bucket above as `lakehouse.tutorial.orders_raw`

```bash
·> python 05_register_orders.py
+----------+------+
|event_date|events|
+----------+------+
|2026-09-01|  4720|
|2026-09-02|  4382|
|2026-09-03|  5479|
|2026-09-04|  4508|
|2026-09-05|  5194|
|2026-09-06|  4962|
|2026-09-07|  4776|
+----------+------+

orders_raw EXTERNAL DELTA s3://lakehouse/tables/orders_raw
```

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  uc_SC["Spark Connect"]:::old
  uc_UC["Unity Catalog<br/>:8081"]:::new
  uc_SW[("SeaweedFS")]:::old
  uc_SC -->|"UC connector"| uc_UC
  uc_UC -.->|"locations,<br/>credentials"| uc_SW
  uc_SC -->|"S3A"| uc_SW
```

# Open formats: read the same table as Iceberg

We can enable Iceberg metadata for the same Delta table with Uniform
```bash
·> python 06_enable_uniform.py
tables/orders_raw/metadata/00000-2a00378b-76c7-4058-96b5-b49d05e2520a.metadata.json
tables/orders_raw/metadata/f2ff3caa-d926-4d7a-a33e-4cc8ef48d5b8-m0.avro
tables/orders_raw/metadata/snap-7000200278295571351-1-f2ff3caa-d926-4d7a-a33e-4cc8ef48d5b8.avro
```

Then we can read as Iceberg clients
```bash
·> python 07_read_iceberg.py
Reading s3://lakehouse/tables/orders_raw/metadata/00000-2a00378b-76c7-4058-96b5-b49d05e2520a.metadata.json

PyIceberg: 34,021 rows (Iceberg v2, converted from Delta version 2)

┌────────────┬────────┐
│ event_date │ events │
│    date    │ int64  │
├────────────┼────────┤
│ 2026-09-01 │   4720 │
│ 2026-09-02 │   4382 │
│ 2026-09-03 │   5479 │
│ 2026-09-04 │   4508 │
│ 2026-09-05 │   5194 │
│ 2026-09-06 │   4962 │
│ 2026-09-07 │   4776 │
└────────────┴────────┘
```

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  ib_SC["Spark Connect"]:::old
  subgraph ib_T["orders_raw"]
    ib_LOG["_delta_log"]:::old
    ib_PQ["Parquet<br/>data files"]:::old
    ib_MD["metadata/<br/>Iceberg"]:::new
  end
  ib_RD["PyIceberg<br/>DuckDB"]:::new
  ib_SC -->|"Delta commit"| ib_LOG
  ib_LOG -.->|"UniForm"| ib_MD
  ib_MD -.->|"lists"| ib_PQ
  ib_RD -->|"reads"| ib_MD
```

# Pipelines: bronze, silver, gold with Spark Declarative Pipelines

> Spark Declarative Pipelines (SDP) lets you declare each table as a query; SDP works out the dependency graph from the tables each query reads and materializes them in order. 

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  pl_SPEC["spark-pipeline.yml<br/>bronze.py, *.sql"]:::new
  pl_SC["Spark Connect"]:::old
  subgraph pl_MD["lakehouse.medallion"]
    pl_BR[("bronze")]:::new
    pl_SI[("silver")]:::new
    pl_GO[("gold")]:::new
  end
  pl_SPEC -->|"graph"| pl_SC
  pl_SC --> pl_BR
  pl_BR --> pl_SI
  pl_SI --> pl_GO
```

```bash
·> ./08_run_pipeline.sh
lakehouse.medallion is ready
2026-11-15 21:24:42: Loading pipeline spec from pipeline/spark-pipeline.yml...
2026-11-15 21:24:42: Creating Spark session...
2026-11-15 21:24:42: Creating dataflow graph...
2026-11-15 21:24:42: Registering graph elements...
...
2026-11-15 21:24:50: Starting run...
2026-11-15 14:25:04: Flow lakehouse.medallion.locations is QUEUED.
2026-11-15 14:25:04: Flow lakehouse.medallion.orders_silver is QUEUED.
2026-11-15 14:25:04: Flow lakehouse.medallion.revenue_by_brand is QUEUED.
2026-11-15 14:25:04: Flow lakehouse.medallion.revenue_by_city_day is QUEUED.
2026-11-15 14:25:04: Flow lakehouse.medallion.orders_bronze is QUEUED.
2026-11-15 14:25:04: Flow lakehouse.medallion.orders_bronze is PLANNING.
2026-11-15 14:25:04: Flow lakehouse.medallion.orders_bronze is STARTING.
2026-11-15 14:25:04: Flow lakehouse.medallion.orders_bronze is RUNNING.
2026-11-15 14:25:04: Flow lakehouse.medallion.locations is PLANNING.
2026-11-15 14:25:04: Flow lakehouse.medallion.locations is STARTING.
2026-11-15 14:25:04: Flow lakehouse.medallion.locations is RUNNING.
2026-11-15 14:25:09: Flow lakehouse.medallion.orders_bronze has COMPLETED.
2026-11-15 14:25:09: Flow lakehouse.medallion.locations has COMPLETED.
2026-11-15 14:25:09: Flow lakehouse.medallion.orders_silver is PLANNING.
2026-11-15 14:25:09: Flow lakehouse.medallion.orders_silver is STARTING.
2026-11-15 14:25:09: Flow lakehouse.medallion.orders_silver is RUNNING.
2026-11-15 14:25:16: Flow lakehouse.medallion.orders_silver has COMPLETED.
2026-11-15 14:25:17: Flow lakehouse.medallion.revenue_by_city_day is PLANNING.
2026-11-15 14:25:17: Flow lakehouse.medallion.revenue_by_city_day is STARTING.
2026-11-15 14:25:17: Flow lakehouse.medallion.revenue_by_city_day is RUNNING.
2026-11-15 14:25:17: Flow lakehouse.medallion.revenue_by_brand is PLANNING.
2026-11-15 14:25:17: Flow lakehouse.medallion.revenue_by_brand is STARTING.
2026-11-15 14:25:17: Flow lakehouse.medallion.revenue_by_brand is RUNNING.
2026-11-15 14:25:21: Flow lakehouse.medallion.revenue_by_city_day has COMPLETED.
2026-11-15 14:25:21: Flow lakehouse.medallion.revenue_by_brand has COMPLETED.
2026-11-15 14:25:23: Run is COMPLETED.
```

The run produce data in bronze, silver, gold tables. We can see as bellow that the bronze table is created identical to the delta table we created above from landing data.

```bash
·> python 09_read_gold.py
=========== BRONZE TABLES ==========
+----------+------+
|event_date|events|
+----------+------+
|2026-09-01|  4720|
|2026-09-02|  4382|
|2026-09-03|  5479|
|2026-09-04|  4508|
|2026-09-05|  5194|
|2026-09-06|  4962|
|2026-09-07|  4776|
+----------+------+

+----------+------+
|event_date|events|
+----------+------+
|2026-09-01|  4720|
|2026-09-02|  4382|
|2026-09-03|  5479|
|2026-09-04|  4508|
|2026-09-05|  5194|
|2026-09-06|  4962|
|2026-09-07|  4776|
+----------+------+

=========== GOLD TABLES ==========
+-------------+------+---------+---------+
|        brand|orders|  revenue|avg_order|
+-------------+------+---------+---------+
|Sushi Express|  2140|160332.61|    74.92|
|  Curry House|  2219|133432.76|    60.13|
| Wok This Way|  2152|118668.13|    55.14|
| Pizza Planet|  2233| 93402.24|    41.83|
|    Taco Loco|  2168| 71548.53|     33.0|
+-------------+------+---------+---------+

locations MANAGED DELTA s3://lakehouse/managed/__unitystorage/catalogs/ca354fda-466e-4e8f-bd08-8abc26629c21/tables/3d71f6a6-6177-4acf-bf43-91b666f27c50
orders_bronze MANAGED DELTA s3://lakehouse/managed/__unitystorage/catalogs/ca354fda-466e-4e8f-bd08-8abc26629c21/tables/f1d85cd3-ca69-41fe-a803-c494b3c2e535
orders_silver MANAGED DELTA s3://lakehouse/managed/__unitystorage/catalogs/ca354fda-466e-4e8f-bd08-8abc26629c21/tables/117a82ed-a1fc-41dc-9e73-0e130117c500
revenue_by_brand MANAGED DELTA s3://lakehouse/managed/__unitystorage/catalogs/ca354fda-466e-4e8f-bd08-8abc26629c21/tables/203a9d4b-1711-4e81-a6e1-8ec1a9eb766c
revenue_by_city_day MANAGED DELTA s3://lakehouse/managed/__unitystorage/catalogs/ca354fda-466e-4e8f-bd08-8abc26629c21/tables/52d7fea9-bb6e-41ca-b85c-93fc08fe5a60
```

# Streaming: Real-Time Mode and micro-batches

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  ks_GEN["generator"]:::new
  ks_K["Kafka :9092<br/>topic orders"]:::new
  subgraph ks_SP["Spark 4.2"]
    ks_SL["stateless<br/>Real-Time Mode"]:::new
    ks_SF["stateful<br/>micro-batch"]:::new
  end
  ks_OUT["Kafka<br/>orders-clean"]:::new
  ks_T[("lakehouse.tutorial<br/>live_totals")]:::new
  ks_GEN --> ks_K
  ks_K --> ks_SL
  ks_K --> ks_SF
  ks_SL -->|"milliseconds"| ks_OUT
  ks_SF -->|"every second"| ks_T
```

## Create Kafka topics

```bash
> docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic orders --partitions 2
Created topic orders.
> docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --topic orders-clean --partitions 2
Created topic orders-clean.
```

## Create checkpoint bucket in S3 for streaming job

```bash
·> python script_s3_create_bucket.py checkpoints
```

## Launch the Kafka consumers
```bash
·> python 10_stream_clean.py
orders -> orders-clean, in Real-Time Mode. Ctrl+C to stop.
```

```bash
·> python 10_stream_totals.py
orders -> lakehouse.tutorial.live_totals, every second. Ctrl+C to stop.
```

## Launch the Producer for generating orders
```bash
·> python 03_generate_orders.py stream
sent 1,000 events
sent 2,000 events
sent 3,000 events
sent 3,746 events to the topic orders
```

## Checking
We can check the message produced in Kafka topic `orders-clean`
```bash
·> docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic orders-clean --from-beginning --max-messages 3
{"event_id":"7b8cfc6245-0","location_id":3,"brand":"Curry House","total":83.14,"received_ms":1794406647325}
{"event_id":"f6e09507cf-0","location_id":1,"brand":"Taco Loco","total":42.44,"received_ms":1794406647392}
{"event_id":"50e87c4f9f-0","location_id":3,"brand":"Pizza Planet","total":14.63,"received_ms":1794406647443}
Processed a total of 3 messages
```

And we can check the checkpoint folders in S3

```bash
·> python script_s3_list_objects.py checkpoints orders_clean | wc -l
     221
·> python script_s3_list_objects.py checkpoints live_totals | wc -l
     221
```

And compare with messages produced in delta table

```bash
·> python 11_check_stream.py
+-----------+------+--------+-----------------------+
|location_id|orders|revenue |last_order_at          |
+-----------+------+--------+-----------------------+
|1          |271   |13893.38|2026-11-18 15:55:01.759|
|2          |336   |17073.44|2026-11-18 15:55:01.875|
|3          |313   |16325.25|2026-11-18 15:55:01.709|
|4          |286   |14751.55|2026-11-18 15:55:01.803|
+-----------+------+--------+-----------------------+

order_created records sent:        1,243
distinct order_created event ids:  1,206
orders in live_totals:             1,206

orders-clean: 1,243 records, p50 1 ms, p99 104 ms in Spark
```


# Trino Set-Up

## Use Hive Metastore as catalog for Delta tables

Requirements: Hive metastore needs extra Jar files for
- postgres client
- AWS sdk bundle for S3 client
- XML config for metastore modules

file `metastore-site.xml`:
```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.s3.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>

    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://seaweedfs:8333</value>
    </property>

    <property>
        <name>fs.s3a.endpoint.region</name>
        <value>us-east-1</value>
    </property>

    <property>
        <name>fs.s3a.access.key</name>
        <value>lakehouse</value>
    </property>

    <property>
        <name>fs.s3a.secret.key</name>
        <value>lakehouse-secret</value>
    </property>

    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>

    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>false</value>
    </property>
</configuration>
```
then the container of Metastore needs to mount the jar files with some settings:

```yaml
services:
  ....
  hive-metastore:
    # image: apache/hive:4.2.1
    image: apache/hive:standalone-metastore-4.2.1
    restart: unless-stopped
    container_name: hive-metastore
    hostname: hive-metastore
    depends_on:
      - hive-postgres
    profiles:
      - trino
    environment:
      HADOOP_OPTIONAL_TOOLS: 'hadoop-aws'
      HADOOP_CLASSPATH: /opt/hadoop/share/hadoop/tools/lib/*
      DB_DRIVER: postgres
      SERVICE_NAME: 'metastore'
      SERVICE_OPTS: >
        -Xmx1G
        -Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
        -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://hive-postgres:5432/metastore_db
        -Djavax.jdo.option.ConnectionUserName=hive
        -Djavax.jdo.option.ConnectionPassword=hive_secret
    ports:
        - '9001:9001'
        - '9083:9083'
    volumes:
        - hive-warehouse:/opt/hive/data/warehouse
        - type: bind
          source: ./hive/ext-jars/postgresql-42.7.14.jar
          target: /opt/hive/lib/postgres.jar
        - type: bind
          source: ./hive/ext-jars/bundle-2.42.25.jar
          target: /opt/hadoop/share/hadoop/common/lib/bundle-2.42.25.jar
        # - ./hive/ext-jars:/tmp/ext-jars:ro
        - ./hive/conf/metastore-site.xml:/opt/hadoop/etc/hadoop/core-site.xml:ro
        - ./hive/conf/metastore-site.xml:/opt/hive/conf/core-site.xml:ro
    networks:
      - hive_net
      - lakehouse_net

```


Create a delta catalog with Hive thrift server

```conf
connector.name=delta_lake
hive.metastore.uri=thrift://hive-metastore:9083

delta.register-table-procedure.enabled=true

fs.s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=lakehouse
s3.aws-secret-key=lakehouse-secret
```

Then we can register a delta table with

```sql
CALL delta_lake.system.register_table(
    schema_name => 'default',
    table_name => 'orders_raw',
    table_location => 's3://lakehouse/tables/orders_raw'
);
```
then query the delta table from Hive catalog in Trino:

```sql
trino> select count(1) from delta_lake.default.orders_raw;
 _col0
-------
 34021
(1 row)

Query 20261119_114518_00002_a52nh, FINISHED, 1 node
Splits: 12 total, 12 done (100.00%)
0.17 [34K rows, 129B] [206K rows/s, 782B/s]
```

### We can create a new schema

```sql
trino> CREATE SCHEMA delta_lake.analytics
    -> WITH (location = 's3://lakehouse/schemas/analytics/');
CREATE SCHEMA
trino> CREATE OR REPLACE TABLE delta_lake.analytics.orders_raw_10
    -> AS select * from delta_lake.default.orders_raw limit 10;
CREATE TABLE: 10 rows
trino> select * from delta_lake.analytics.orders_raw_10;
   event_id   |  event_type   |         ts          | location_id |  order_id  |                          body                         >
--------------+---------------+---------------------+-------------+------------+------------------------------------------------------->
 6754a26b65-0 | order_created | 2026-09-04T14:27:51 |           2 | 6754a26b65 | {"brand": "Taco Loco", "items": 3, "total": 41.69}    >
 6754a26b65-1 | order_ready   | 2026-09-04T14:38:51 |           2 | 6754a26b65 | {"brand": "Taco Loco", "items": 3, "total": 41.69}    >
 6754a26b65-2 | delivered     | 2026-09-04T15:08:51 |           2 | 6754a26b65 | {"brand": "Taco Loco", "items": 3, "total": 41.69}    >
 bf755aaa84-0 | order_created | 2026-09-04T16:01:48 |           1 | bf755aaa84 | {"brand": "Sushi Express", "items": 2, "total": 62.03}>
 bf755aaa84-1 | order_ready   | 2026-09-04T16:17:48 |           1 | bf755aaa84 | {"brand": "Sushi Express", "items": 2, "total": 62.03}>
 bf755aaa84-2 | delivered     | 2026-09-04T16:46:48 |           1 | bf755aaa84 | {"brand": "Sushi Express", "items": 2, "total": 62.03}>
 bf755aaa84-2 | delivered     | 2026-09-04T16:46:48 |           1 | bf755aaa84 | {"brand": "Sushi Express", "items": 2, "total": 62.03}>
 49995e1869-0 | order_created | 2026-09-04T21:23:15 |           1 | 49995e1869 | {"brand": "Pizza Planet", "items": 2, "total": 33.97} >
 49995e1869-1 | order_ready   | 2026-09-04T21:39:15 |           1 | 49995e1869 | {"brand": "Pizza Planet", "items": 2, "total": 33.97} >
 49995e1869-2 | delivered     | 2026-09-04T21:50:15 |           1 | 49995e1869 | {"brand": "Pizza Planet", "items": 2, "total": 33.97} >
(10 rows)
```

The delta table was automatically created in the schema folder:

```sql
trino> show create table delta_lake.analytics.orders_raw_10;
                                          Create Table
-------------------------------------------------------------------------------------------------
 CREATE TABLE delta_lake.analytics.orders_raw_10 (
    event_id varchar,
    event_type varchar,
    ts varchar,
    location_id bigint,
    order_id varchar,
    body varchar,
    event_date date
 )
 WITH (
    location = 's3://lakehouse/schemas/analytics/orders_raw_10-75b4066b016b46e39bd5b0f11d98ae15'
 )
(1 row)
```

## Delta table managed by Unity Catalog

Note that we cannot register the tables managed by Unity Catalog into the new Hive catalog.

## Iceberg REST catalog

Seaweedfs supports a REST API for serving Iceberg catalog.
But we need a Table S3 bucket, not a ordinary S3 bucket

```bash
·> python script_s3table_create_bucket.py iceberg-data
{'ResponseMetadata': {'RequestId': '18E97080C0E6AE0E41471EEA', 'HostId': '', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'server': 'SeaweedFS 30GB 4.53', 'x-amz-request-id': '18E97080C0E6AE0E41471EEA', 'date': 'Thu, 19 Nov 2026 12:00:24 GMT', 'content-length': '67'}, 'RetryAttempts': 0}, 'arn': 'arn:aws:s3tables:us-east-1:lakehouse:bucket/iceberg-data'}
```

The make a iceberg catalog with it

```conf
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://seaweedfs:8181


# fs.native-s3.enabled=true
# fs.hadoop.enabled=false
fs.s3.enabled=true
s3.endpoint=http://seaweedfs:8333
s3.region=us-east-1
iceberg.rest-catalog.warehouse=s3://iceberg-data
iceberg.rest-catalog.security=SIGV4
s3.signer-type=AwsS3V4Signer
s3.aws-access-key=lakehouse
s3.aws-secret-key=lakehouse-secret

s3.path-style-access=true
iceberg.file-format=PARQUET
iceberg.unique-table-location=true

# Optimized materialized views to cleanup metadata and snapshots
iceberg.delete-after-commit-enabled=true
iceberg.max-previous-versions=1
iceberg.incremental-refresh-enabled=false
iceberg.expire-snapshots.min-retention=1m
iceberg.remove-orphan-files.min-retention=1m
iceberg.materialized-views.refresh-snapshot-retention-period=1m
iceberg.register-table-procedure.enabled=true
```

The we can create a schema with it

```sql
trino> CREATE SCHEMA iceberg.test_schema;
CREATE SCHEMA
trino> show create schema iceberg.test_schema;
                 Create Schema
-----------------------------------------------
 CREATE SCHEMA iceberg.test_schema
 WITH (
    location = 's3://iceberg-data/test_schema'
 )
(1 row)

Query 20261119_123212_00011_a52nh, FINISHED, 1 node
```

And create tables

```sql
trino> CREATE TABLE iceberg.test_schema.orders_raw_10 (
    ->     event_id varchar,
    ->     event_type varchar,
    ->     ts varchar,
    ->     location_id bigint,
    ->     order_id varchar,
    ->     body varchar,
    ->     event_date date
    -> );
    ->
CREATE TABLE
trino> Insert into iceberg.test_schema.orders_raw_10
    -> select * from delta_lake.default.orders_raw limit 10;
    ->
INSERT: 10 rows
```

we can even create another table bucket and create a schema in it

```bash
·> python script_s3table_create_bucket.py trino-iceberg
{'ResponseMetadata': {'RequestId': '18E9746F62B1C853758814B5', 'HostId': '', 'HTTPStatusCode': 200, 'HTTPHeaders': {'content-type': 'application/x-amz-json-1.1', 'server': 'SeaweedFS 30GB 4.53', 'x-amz-request-id': '18E9746F62B1C853758814B5', 'date': 'Thu, 19 Nov 2026 13:12:28 GMT', 'content-length': '68'}, 'RetryAttempts': 0}, 'arn': 'arn:aws:s3tables:us-east-1:lakehouse:bucket/trino-iceberg'}
```

```sql
trino> CREATE SCHEMA iceberg.analytics
    -> WITH (location = 's3://trino-iceberg/schemas/analytics/');
    ->
CREATE SCHEMA
```

but this schema is not usable. It seems that the catalog supports only the schemas created in the same catalog bucket.

# Orchestration: Airflow runs the pipeline and the maintenance

## Run the SDP pipeline from Airflow

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  af_AF["Airflow :8085<br/>two DAGs"]:::new
  af_SC["Spark Connect"]:::old
  af_UC["Unity Catalog"]:::old
  af_T[("medallion and<br/>tutorial tables")]:::old
  af_AF -->|"SDP run, OPTIMIZE"| af_SC
  af_SC -->|"names"| af_UC
  af_SC --> af_T
```

```bash
·> docker compose exec airflow airflow dags unpause medallion
dag_id    | is_paused
==========+==========
medallion | True

·> docker compose exec airflow airflow dags unpause delta_maintenance
dag_id            | is_paused
==================+==========
delta_maintenance | True
```

```bash
·> docker compose exec airflow airflow dags list-runs medallion -o plain
dag_id     run_id                                    state    run_after                         logical_date               start_date                        end_date
medallion  manual__2026-11-11T15:26:49.565888+00:00  success  2026-11-11T15:26:49.565888+00:00  2026-11-11T15:26:45+00:00  2026-11-11T15:26:51.007140+00:00  2026-11-11T15:36:04.505733+00:00

·> docker compose exec airflow airflow dags list-runs delta_maintenance -o plain
dag_id             run_id                                    state    run_after                         logical_date               start_date                        end_date
delta_maintenance  manual__2026-11-11T15:36:45.350134+00:00  success  2026-11-11T15:36:45.350134+00:00  2026-11-11T15:36:43+00:00  2026-11-11T15:36:46.861085+00:00  2026-11-11T15:37:29.911793+00:00

·> docker compose exec airflow grep -rhoE "revenue_by_brand has [0-9]+ rows|lakehouse[.a-z_]+: [0-9]+ files -> [0-9]+" /opt/airflow/logs
revenue_by_brand has 5 rows
lakehouse.tutorial.orders_raw: 1 files -> 1
lakehouse.tutorial.live_totals: 1 files -> 1
lakehouse.tutorial.orders_raw: 3 files -> 1
```

## A pipeline for external tables of Unity Catalog

![Airflow Non UC Dag](./airflow-non-uc-dag.png)

Then we can register the tables into Trino catalog

```sql
trino> create schema delta_lake.medallion
trino> CALL delta_lake.system.register_table(
    ->     schema_name => 'medallion',
    ->     table_name => 'orders_bronze',
    ->     table_location => 's3://lakehouse/tables/bronze/orders'
    -> );
trino> select count(*) from delta_lake.medallion.orders_bronze;
 _col0
-------
 34021
(1 row)
trino> CALL delta_lake.system.register_table(
    ->     schema_name => 'medallion',
    ->     table_name => 'locations_bronze',
    ->     table_location => 's3://lakehouse/tables/bronze/new_locations'
    -> );
trino> select * from delta_lake.medallion.locations_bronze;
 location_id |     city
-------------+---------------
           1 | Oakland
           2 | Berkeley
           3 | San Francisco
           4 | San Jose
trino> CALL delta_lake.system.register_table(
    ->     schema_name => 'medallion',
    ->     table_name => 'revenue_by_brand_gold',
    ->     table_location => 's3://lakehouse/tables/gold/revenue_by_brand'
    -> );
trino> select * from delta_lake.medallion.revenue_by_brand_gold;
     brand     | orders |  revenue  | avg_order
---------------+--------+-----------+-----------
 Curry House   |   2219 | 133432.76 |     60.13
 Taco Loco     |   2168 |  71548.53 |      33.0
 Pizza Planet  |   2233 |  93402.24 |     41.83
 Sushi Express |   2140 | 160332.61 |     74.92
 Wok This Way  |   2152 | 118668.13 |     55.14
(5 rows)
```

# ML: MLflow tracks what you compute

```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  ml_JOB["PySpark job"]:::new
  ml_SC["Spark Connect"]:::old
  ml_ML["MLflow :5000"]:::new
  ml_SW[("SeaweedFS")]:::old
  ml_JOB -->|"reads gold"| ml_SC
  ml_JOB -->|"logs run"| ml_ML
  ml_ML -->|"artifacts"| ml_SW
```

```bash
·> python log_run.py
2026/11/11 22:41:51 INFO mlflow.tracking.fluent: Experiment with name 'ghost-kitchen' does not exist. Creating a new experiment.
I1111 22:41:51.591965 5983474 ev_poll_posix.cc:588] FD from fork parent still in poll list: fd(7, generation: 1)
I1111 22:41:51.603270 5983499 ev_poll_posix.cc:588] FD from fork parent still in poll list: fd(7, generation: 1)
I1111 22:41:51.615488 5983528 ev_poll_posix.cc:588] FD from fork parent still in poll list: fd(7, generation: 1)
🏃 View run brand-revenue at: http://localhost:5001/#/experiments/1/runs/28f9824e6c294a7e9ffa33f6d39b2092
🧪 View experiment at: http://localhost:5001/#/experiments/1
brand-revenue: FINISHED
  best_avg_order = 74.92
  brands = 5.0
  revenue = 577384.27
mlflow/1/28f9824e6c294a7e9ffa33f6d39b2092/artifacts/revenue_by_brand.csv

·> python read_csv_s3.py lakehouse "mlflow/1/28f9824e6c294a7e9ffa33f6d39b2092/artifacts/revenue_by_brand.csv"
           brand  orders    revenue  avg_order
0    Curry House    2219  133432.76      60.13
1      Taco Loco    2168   71548.53      33.00
2   Pizza Planet    2233   93402.24      41.83
3   Wok This Way    2152  118668.13      55.14
4  Sushi Express    2140  160332.61      74.92
```

# UC UI

Checkout unitycatalog repo from github. Go to ui-new folder

```bash
> git clone https://github.com/unitycatalog/unitycatalog.git
> cd ui-new
```

## Server bridge

```
server $> UC_SERVER=http://localhost:8081 PORT=8082 cargo run
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.21s
     Running `/Users/minh-dungdang/Projects/aws-certificates/lakehouses-bigdata/unitycatalog/ui-new/server/target/debug/uc-ui-bridge`
2026-11-11T16:40:57.481513Z  INFO uc_ui_bridge: Unity Catalog UI bridge listening on 0.0.0.0:8082 uc_server=http://localhost:8081 auth_enabled=false

web $> bun install
bun install v1.4.4 (38afee6c8)
  🔍 Resolving...

web ·> VITE_API_TARGET=http://localhost:8082 bun run dev
```

![Unity catalog UI](./unitycatalog-ui.png)


```mermaid
flowchart LR
  classDef new fill:#d97706,fill-opacity:0.72,stroke:#d97706,color:#ffffff
  classDef old fill:#4b5563,fill-opacity:0.72,stroke:#4b5563,color:#ffffff
  af_AF["UC UI :5173<br/>React SPA"]:::new
  af_SC["UC UI Server :8082<br/>UC Bridge"]:::old
  af_UC["UC :8081"<br/>container :8080]:::old
  af_AF -->|"REST"| af_SC
  af_SC -->|"REST"| af_UC
```


# Docker Compose with Profiles

We can launch docker compose services with profiles, for example for seaweedfs storage only:
```bash
> docker compose --profile storage up -d --wait
```
and stop it

```bash
> docker compose --profile storage down
```

We can stop all profiles

```bash
·> docker compose --profile "*" down
```

Or we can launch services excluding some profiles

```
> COMPOSE_PROFILES=$(docker compose config --profiles | grep -vE '^(airflow|mlflow|streaming)$' | paste -sd, -) docker compose up -d --wait
```