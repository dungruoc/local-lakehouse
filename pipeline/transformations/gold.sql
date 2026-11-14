-- Gold: the numbers people ask for, built from silver.
CREATE MATERIALIZED VIEW revenue_by_brand
USING delta
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported',
               'delta.checkpoint.writeStatsAsJson' = 'true',
               'delta.checkpoint.writeStatsAsStruct' = 'true')
AS SELECT
  brand,
  count(*) AS orders,
  round(sum(total), 2) AS revenue,
  round(avg(total), 2) AS avg_order
FROM orders_silver
WHERE event_type = 'order_created'
GROUP BY brand;
 
CREATE MATERIALIZED VIEW revenue_by_city_day
USING delta
TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported',
               'delta.checkpoint.writeStatsAsJson' = 'true',
               'delta.checkpoint.writeStatsAsStruct' = 'true')
AS SELECT
  city,
  to_date(event_time) AS day,
  count(*) AS orders,
  round(sum(total), 2) AS revenue
FROM orders_silver
WHERE event_type = 'order_created'
GROUP BY city, to_date(event_time);