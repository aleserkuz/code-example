UPDATE mart.d_customer dc
SET 
  first_name = uol.first_name,
  last_name  = uol.last_name,
  city_id = uol.city_id 
FROM staging.user_order_log uol
WHERE
  dc.customer_id = uol.customer_id AND
  uol.date_time = '{{ ds }}' AND
    (dc.last_name <> uol.last_name OR 
     dc.first_name <> uol.first_name OR
     dc.city_id <> uol.city_id);


INSERT INTO mart.d_customer   
SELECT
  DISTINCT
  a.customer_id, a.first_name, a.last_name, max(a.city_id)
FROM staging.user_order_log a
LEFT JOIN mart.d_customer b 
ON a.customer_id = b.customer_id
WHERE a.date_time = '{{ ds }}' AND b.customer_id IS NULL
GROUP BY a.customer_id, a.first_name, a.last_name;