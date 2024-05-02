TRUNCATE mart.f_daily_sales CASCADE;


INSERT INTO mart.f_daily_sales  (date_id, item_id, customer_id , price , quantity , amount)
SELECT
  date_id,
  item_id,
  customer_id,
  AVG(payment_amount / quantity) AS price,
  SUM(quantity) AS quantity,
  SUM(payment_amount) AS payment_amount
FROM staging.user_order_log AS ual
JOIN mart.d_calendar dc ON to_date(ual.date_time::TEXT,'YYYY-MM-DD') = dc.fact_date
GROUP BY date_id, item_id, customer_id
ORDER BY date_id;