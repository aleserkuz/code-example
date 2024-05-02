ALTER TABLE staging.user_order_log ADD COLUMN "status" varchar(30);

UPDATE staging.user_order_log 
SET 
  status = 'shipped'
WHERE status IS NULL;
