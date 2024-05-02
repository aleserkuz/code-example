TRUNCATE mart.d_calendar CASCADE;


with all_dates as (
        select distinct to_date(date_time::TEXT,'YYYY-MM-DD') as date_time FROM staging.user_activity_log
        union
        select distinct to_date(date_time::TEXT,'YYYY-MM-DD') FROM staging.user_order_log
        union
        select distinct to_date(date_id::TEXT,'YYYY-MM-DD') FROM staging.customer_research
        order by date_time
        )
INSERT INTO mart.d_calendar
SELECT
    nextval('mart.d_calendar_date_id_seq')
    ,date_time as fact_date
    ,EXTRACT(day FROM date_time)::INT as day_num
    ,EXTRACT(month FROM date_time)::INT as month_num
    ,to_char(date_time, 'Mon') as month_name
    ,EXTRACT(year FROM date_time)::INT as year_num
FROM all_dates
ORDER BY date_time;
