with new_date as (
  SELECT '{{ ds }}'::timestamp without time zone as date_time
)
INSERT INTO mart.d_calendar
SELECT
    nextval('mart.d_calendar_date_id_seq')
    ,date_time as fact_date
    ,EXTRACT(day from date_time)::INT as day_num
    ,EXTRACT(month from date_time)::INT as month_num
    ,to_char(date_time, 'Mon') as month_name
    ,EXTRACT(year from date_time)::INT as year_num
from new_date
where date_time not in (select fact_date from mart.d_calendar);
