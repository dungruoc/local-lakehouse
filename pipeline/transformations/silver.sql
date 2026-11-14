-- Silver: each event once, with a known location, the order details parsed
-- out of the JSON body, and the city attached.
CREATE MATERIALIZED VIEW orders_silver
USING delta
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported',
               'delta.checkpoint.writeStatsAsJson' = 'true',
               'delta.checkpoint.writeStatsAsStruct' = 'true')
AS SELECT DISTINCT
  o.event_id,
  o.event_type,
  to_timestamp(o.ts) AS event_time,
  o.order_id,
  l.city,
  get_json_object(o.body, '$.brand') AS brand,
  CAST(get_json_object(o.body, '$.total') AS DOUBLE) AS total
FROM orders_bronze o
JOIN locations l ON o.location_id = l.location_id;