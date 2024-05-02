TRUNCATE mart.f_activity CASCADE;


INSERT INTO mart.f_activity (activity_id, date_id, click_number)
SELECT 
  action_id AS activity_id,
  date_id,
  SUM(quantity) click_number
FROM staging.user_activity_log AS ual
JOIN mart.d_calendar dc ON to_date(ual.date_time::TEXT,'YYYY-MM-DD') = dc.fact_date
GROUP BY action_id, date_id
ORDER BY action_id, date_id;