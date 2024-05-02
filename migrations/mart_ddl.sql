CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.f_customer_retention;
DROP TABLE IF EXISTS mart.f_daily_sales;
DROP TABLE IF EXISTS mart.f_activity;
DROP TABLE IF EXISTS mart.d_category;
DROP TABLE IF EXISTS mart.d_city;
DROP TABLE IF EXISTS mart.d_item;
DROP TABLE IF EXISTS mart.d_customer;
DROP TABLE IF EXISTS mart.d_calendar;


CREATE TABLE mart.d_calendar(
    date_id SERIAL PRIMARY KEY, 
    fact_date TIMESTAMP,
    day_num SMALLINT, 
    month_num SMALLINT, 
    month_name VARCHAR(8),
    year_num SMALLINT
);

CREATE TABLE mart.d_customer(
    customer_id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    city_id INTEGER
);

CREATE TABLE mart.d_item(
    item_id INTEGER,
    item_name VARCHAR(50),
    category_id INTEGER
);

CREATE TABLE mart.d_city(
    сity_id INTEGER,
    city_name VARCHAR(50)
);

CREATE TABLE mart.d_category(
    category_id INTEGER,
    category_name VARCHAR(50)
);

CREATE TABLE mart.f_activity(
    activity_id INTEGER,
    date_id INTEGER,
    click_number BIGINT
);

CREATE TABLE mart.f_daily_sales(
    date_id INTEGER,
    item_id INTEGER,
    customer_id INTEGER,
    price NUMERIC(10,2),
    quantity NUMERIC(10,2),
    amount NUMERIC(10,2)
);

CREATE TABLE mart.f_customer_retention(
    new_customers_count INTEGER,
    returning_customers_count INTEGER,
    refunded_customer_count INTEGER,
    period_name VARCHAR(20),
    period_id INTEGER,
    item_id INTEGER,
    new_customers_revenue NUMERIC(10,2),
    returning_customers_revenue NUMERIC(10,2),
    customers_refunded INTEGER
);

COMMIT;


