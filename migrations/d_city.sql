TRUNCATE mart.d_city CASCADE;


INSERT INTO mart.d_city 
SELECT 
  DISTINCT
  city_id,
  city_name
FROM staging.user_order_log
GROUP BY city_id, city_name
ORDER BY city_id;