TRUNCATE mart.f_daily_sales CASCADE;

INSERT INTO mart.f_daily_sales (date_id, item_id, customer_id, price, quantity, amount)
SELECT 
  dc.date_id
  ,item_id
  ,customer_id
  ,AVG(payment_amount/quantity) AS price
  ,SUM(quantity) AS quantity
  ,SUM(CASE WHEN "status" = 'refunded' THEN -1*payment_amount ELSE payment_amount END) AS amount
FROM staging.user_order_log uol
JOIN mart.d_calendar as dc on uol.date_time = dc.fact_date 
GROUP BY dc.date_id, item_id, customer_id
ORDER BY dc.date_id DESC, item_id, customer_id;

