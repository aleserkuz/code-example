TRUNCATE mart.f_customer_retention CASCADE;

INSERT INTO mart.f_customer_retention
WITH 
new_customers as(
SELECT
  date_part('week', date_time::date) AS week_id
  ,item_id
  ,customer_id
  ,COUNT(1) AS orders
  ,SUM(payment_amount) AS amount
FROM staging.user_order_log uol
GROUP BY week_id, item_id, customer_id
HAVING COUNT(1) = 1
),
new_customers_aggregate as(
SELECT 
  week_id
  ,item_id
  ,COUNT(1) AS new_customers_count
  ,SUM(amount) AS new_customers_revenue
FROM new_customers
GROUP BY week_id, item_id
),
returning_customers AS(
SELECT
  date_part('week', date_time::date) AS week_id
  ,item_id
  ,customer_id
  ,COUNT(1) AS orders
  ,SUM(payment_amount) AS amount
FROM staging.user_order_log uol
GROUP BY week_id, item_id, customer_id
HAVING COUNT(1) > 1
),
returning_customers_aggregate AS(
SELECT 
  week_id
  ,item_id
  ,COUNT(1) AS returning_customers_count
  ,SUM(amount) AS returning_customers_revenue
FROM returning_customers rc
GROUP BY week_id, item_id
),
refunded_customer AS (
SELECT
  date_part('week', date_time::date) AS week_id
  ,item_id
  ,customer_id
  ,COUNT(1) AS orders
FROM staging.user_order_log uol
WHERE status = 'refunded'
GROUP BY week_id, item_id, customer_id
),
refunded_customer_aggregate AS(
SELECT 
  week_id
  ,item_id
  ,COUNT(1) AS refunded_customer_count
  ,SUM(orders) AS customers_refunded
FROM refunded_customer
GROUP BY week_id, item_id
)
SELECT 
  nca.new_customers_count
  ,rca.returning_customers_count
  ,rca2.refunded_customer_count
  ,'weekly' AS "period_name"
  ,nca.week_id
  ,nca.item_id
  ,nca.new_customers_revenue
  ,rca.returning_customers_revenue
  ,rca2.customers_refunded
FROM new_customers_aggregate nca
JOIN returning_customers_aggregate rca
ON nca.week_id = rca.week_id AND nca.item_id = rca.item_id
LEFT JOIN refunded_customer_aggregate rca2
ON nca.week_id = rca2.week_id AND nca.item_id = rca2.item_id
ORDER BY nca.week_id ,nca.item_id;

