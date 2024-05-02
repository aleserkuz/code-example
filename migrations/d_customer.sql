TRUNCATE mart.d_customer CASCADE;


WITH a AS (
SELECT customer_id, max(date_time) AS dt
FROM staging.user_order_log GROUP BY customer_id
)
INSERT INTO mart.d_customer
SELECT
  DISTINCT
  a.customer_id,
  first_name,
  last_name,
  max(b.city_id)
FROM staging.user_order_log AS b
JOIN a ON b.customer_id = a.customer_id AND b.date_time = a.dt
GROUP BY a.customer_id, first_name, last_name
ORDER BY a.customer_id;