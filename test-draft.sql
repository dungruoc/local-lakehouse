CALL hive.system.register_table(
    schema_name => 'default',
    table_name => 'orders_raw',
    table_location => 's3://lakehouse/tables/orders_raw',
    metadata_file_name => '00000-5fffed41-32f0-4166-9871-7f8a32c639b3.metadata.json'
);


CALL delta_lake.system.register_table(
    schema_name => 'default',
    table_name => 'orders_raw',
    table_location => 's3://lakehouse/tables/orders_raw'
);



CREATE SCHEMA hive.test_schema
WITH (location = 's3://trino-iceberg/schemas/test_schema/');


CREATE TABLE hive.test_schema.yearly_clicks (
    year,
    clicks
)
WITH (
    partitioning = ARRAY['year']
)
AS VALUES
    (2021, 10000),
    (2022, 20000);


CALL hive.system.register_table(
    schema_name => 'default',
    table_name => 'orders_raw',
    table_location => 's3://lakehouse/tables/orders_raw',
    metadata_file_name => '00000-5fffed41-32f0-4166-9871-7f8a32c639b3.metadata.json'
);


CREATE MATERIALIZED VIEW hive.test_schema.orders_raw_10
AS select * from hive.default.orders_raw limit 10;


hadoop fs -ls s3://trino-iceberg/schemas/

hadoop fs -mkdir -p s3://trino-iceberg/schemas/test_schema

hadoop classpath --glob | tr ':' '\n' | grep -E 'hadoop-aws|bundle'

docker compose exec hive-metastore sh -c 'ps -ef | grep -E "[M]etastore|[m]etastore"'

Then inspect its environment (replace PID):
docker compose exec hive-metastore sh -c \
  'tr "\0" "\n" < /proc/{PID}/environ | grep -E "HADOOP|METASTORE|HIVE"'

docker compose up -d --force-recreate hive-metastore