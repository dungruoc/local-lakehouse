CALL delta_lake.system.register_table(
    schema_name => 'default',
    table_name => 'orders_raw',
    table_location => 's3://lakehouse/tables/orders_raw'
);

CREATE SCHEMA delta_lake.analytics
WITH (location = 's3://lakehouse/schemas/analytics/');

CREATE OR REPLACE TABLE delta_lake.analytics.orders_raw_10
AS select * from delta_lake.default.orders_raw limit 10;


CALL delta_lake.system.register_table(
    schema_name => 'analytics',
    table_name => 'orders_bronze',
    table_location => 's3://lakehouse/managed/__unitystorage/catalogs/ca354fda-466e-4e8f-bd08-8abc26629c21/tables/f1d85cd3-ca69-41fe-a803-c494b3c2e535'
);
-> failed


CALL delta_lake.system.register_table(
    schema_name => 'medallion',
    table_name => 'orders_bronze',
    table_location => 's3://lakehouse/tables/bronze/orders'
);

CALL delta_lake.system.register_table(
    schema_name => 'medallion',
    table_name => 'locations_bronze',
    table_location => 's3://lakehouse/tables/bronze/new_locations'
);

CALL delta_lake.system.register_table(
    schema_name => 'medallion',
    table_name => 'revenue_by_brand_gold',
    table_location => 's3://lakehouse/tables/gold/revenue_by_brand'
);



CALL delta_lake.system.register_table(
    schema_name => 'analytics',
    table_name => 'orders_raw_10_bis',
    table_location => 's3://lakehouse/schemas/analytics/orders_raw_10-73fd14e425ca4af3a0642076d3f6bda0'
);

trino> show create table delta_lake.analytics.orders_raw_10_bis;
                                          Create Table
-------------------------------------------------------------------------------------------------
 CREATE TABLE delta_lake.analytics.orders_raw_10_bis (
    event_id varchar,
    event_type varchar,
    ts varchar,
    location_id bigint,
    order_id varchar,
    body varchar,
    event_date date
 )
 WITH (
    location = 's3://lakehouse/schemas/analytics/orders_raw_10-73fd14e425ca4af3a0642076d3f6bda0'
 )
(1 row)

INSERT INTO delta_lake.analytics.orders_raw_10_bis
SELECT * 
FROM delta_lake.analytics.orders_raw_10_bis
WHERE event_id = '7af0c87bd0-1';

CALL delta_lake.system.register_table(
    schema_name => 'analytics',
    table_name => 'orders_bronze',
    table_location => 's3://lakehouse/managed/__unitystorage/catalogs/db429ef6-ea85-42d7-9264-6056d517f830/tables/3dd1777e-4ac4-48bf-a1cd-9f4d13b589b7'
);


CALL delta_lake.system.register_table(
    schema_name => 'analytics',
    table_name => 'live_totals',
    table_location => 's3://lakehouse/tables/live_totals'
);

CREATE SCHEMA iceberg.test_schema;

CALL iceberg.system.register_table(
    schema_name => 'test_schema',
    table_name => 'orders_raw',
    table_location => 's3://lakehouse/tables/orders_raw',
    metadata_file_name => '00000-2a00378b-76c7-4058-96b5-b49d05e2520a.metadata.json'
);

CREATE TABLE iceberg.test_schema.orders_raw_10 (
    event_id varchar,
    event_type varchar,
    ts varchar,
    location_id bigint,
    order_id varchar,
    body varchar,
    event_date date
);


Insert into iceberg.test_schema.orders_raw_10
select * from delta_lake.default.orders_raw limit 10;


CREATE SCHEMA iceberg.analytics
WITH (location = 's3://trino-iceberg/schemas/analytics/');


CALL iceberg.system.register_table(
    schema_name => 'analytics',
    table_name => 'orders_raw',
    table_location => 's3://lakehouse/tables/orders_raw',
    metadata_file_name => '00000-2a00378b-76c7-4058-96b5-b49d05e2520a.metadata.json'
);
=> Fail


CREATE TABLE iceberg.analytics.orders_raw_10 (
    event_id varchar,
    event_type varchar,
    ts varchar,
    location_id bigint,
    order_id varchar,
    body varchar,
    event_date date
);


CALL iceberg.system.register_table(
  schema_name => 'analytics', 
  table_name => 'orders_raw', 
  table_location => 's3://lakehouse/tables/orders_raw',
  metadata_file_name => '00000-5fffed41-32f0-4166-9871-7f8a32c639b3.metadata.json'
  );


CREATE SCHEMA iceberg.test_schema
WITH (location = 's3://test-s3tables-bucket/schema');


CREATE MATERIALIZED VIEW iceberg.test_schema.orders_raw_10
AS select * from delta_lake.default.orders_raw limit 10;
-- failed: createMaterializedView is not supported for Iceberg REST catalog

CREATE OR REPLACE TABLE iceberg.test_schema.orders_raw_10
AS select * from delta_lake.default.orders_raw limit 10;
-- Query 20261115_040441_00007_nuw3r failed: Failed to create transaction


-- rino> show create table delta_lake.default.orders_raw;
--                    Create Table
-- --------------------------------------------------
--  CREATE TABLE delta_lake.default.orders_raw (
--     event_id varchar,
--     event_type varchar,
--     ts varchar,
--     location_id bigint,
--     order_id varchar,
--     body varchar,
--     event_date date
--  )
--  WITH (
--     column_mapping_mode = 'NAME',
--     location = 's3://lakehouse/tables/orders_raw'
--  )
-- (1 row)


create table iceberg.test_schema.orders_raw_10 (
    event_id varchar,
    event_type varchar,
    ts varchar,
    location_id bigint,
    order_id varchar,
    body varchar,
    event_date date
 );
-- Failed

CREATE MATERIALIZED VIEW iceberg.analytics.orders_raw_10
AS select * from delta_lake.default.orders_raw limit 10;
-- Failed

CREATE OR REPLACE TABLE iceberg.analytics.orders_raw_10
AS select * from delta_lake.default.orders_raw limit 10;

-- trino> CREATE OR REPLACE TABLE iceberg.analytics.orders_raw_10
--     -> AS select * from delta_lake.default.orders_raw limit 10;
--     ->
-- CREATE TABLE: 10 rows
 


CALL iceberg.system.register_table(
  schema_name => 'test_schema', 
  table_name => 'orders_raw', 
  table_location => 's3://lakehouse/tables/orders_raw',
  metadata_file_name => '00000-5fffed41-32f0-4166-9871-7f8a32c639b3.metadata.json'
  );

-- => Failed


CREATE MATERIALIZED VIEW hive.test_schema.orders_raw_10
AS select * from hive.default.orders_raw limit 10;

CALL iceberg.system.register_table(
  schema_name => 'analytics', 
  table_name => 'orders_raw', 
  table_location => 's3://lakehouse/tables/orders_raw',
  metadata_file_name => '00000-5fffed41-32f0-4166-9871-7f8a32c639b3.metadata.json'
  );


hadoop fs -ls s3://trino-iceberg/schemas/

hadoop fs -mkdir -p s3://trino-iceberg/schemas/test_schema

hadoop classpath --glob | tr ':' '\n' | grep -E 'hadoop-aws|bundle'

docker compose exec hive-metastore sh -c 'ps -ef | grep -E "[M]etastore|[m]etastore"'

Then inspect its environment (replace PID):
docker compose exec hive-metastore sh -c \
  'tr "\0" "\n" < /proc/{PID}/environ | grep -E "HADOOP|METASTORE|HIVE"'

docker compose up -d --force-recreate hive-metastore