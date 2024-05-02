TRUNCATE mart.d_item CASCADE;

with last_item_names as (
	SELECT
	  DISTINCT
	  item_id,
	  MAX(date_time) AS last_date
	FROM staging.user_order_log
	GROUP BY item_id
	ORDER BY item_id)
INSERT INTO mart.d_item
SELECT
  DISTINCT
  uol.item_id,
  uol.item_name
FROM staging.user_order_log uol, last_item_names ltn
WHERE 
  uol.date_time = ltn.last_date 
  AND uol.item_id = ltn.item_id
ORDER BY uol.item_id;