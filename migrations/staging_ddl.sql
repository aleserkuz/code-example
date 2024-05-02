CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.user_order_log;
DROP TABLE IF EXISTS staging.user_activity_log;
DROP TABLE IF EXISTS staging.customer_research;
DROP TABLE IF EXISTS staging.price_log;


CREATE TABLE staging.user_order_log(
    id SERIAL,
    date_time TIMESTAMP,
    city_id INTEGER,
    city_name VARCHAR(50),
    customer_id BIGINT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    item_id INTEGER,
    item_name VARCHAR(50),
    quantity BIGINT,
    payment_amount NUMERIC(10,2),
    PRIMARY KEY (id)
);


CREATE TABLE staging.user_activity_log(
    id SERIAL,
    date_time TIMESTAMP,
    action_id BIGINT,
    customer_id BIGINT,
    quantity BIGINT,
    PRIMARY KEY (id)
);

CREATE TABLE staging.customer_research(
    id SERIAL, 
    date_id TIMESTAMP,
    category_id INTEGER,
    geo_id INTEGER,
    sales_qty BIGINT,
    sales_amt NUMERIC(10,2),
    PRIMARY KEY (id)
);

CREATE TABLE staging.price_log(
    id SERIAL, 
    item_name VARCHAR(50),
    price NUMERIC(10,2),
    PRIMARY KEY (id)
);

COMMIT;

