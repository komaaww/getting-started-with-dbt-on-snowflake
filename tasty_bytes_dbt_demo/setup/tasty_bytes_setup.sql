USE ROLE accountadmin;


---CREATE OR REPLACE WAREHOUSE tasty_bytes_dbt_wh
---    WAREHOUSE_SIZE = 'small'
---    WAREHOUSE_TYPE = 'standard'
---    AUTO_SUSPEND = 60
---    AUTO_RESUME = TRUE
---    INITIALLY_SUSPENDED = TRUE
---    COMMENT = 'warehouse for tasty bytes dbt demo';


--USE WAREHOUSE tasty_bytes_dbt_wh;

--CREATE DATABASE IF NOT EXISTS tasty_bytes_dbt_db;
CREATE OR REPLACE SCHEMA tasty_bytes_dbt_db.raw;
CREATE OR REPLACE SCHEMA tasty_bytes_dbt_db.dev;
CREATE OR REPLACE SCHEMA tasty_bytes_dbt_db.prod;


ALTER SCHEMA tasty_bytes_dbt_db.dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA tasty_bytes_dbt_db.dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA tasty_bytes_dbt_db.dev SET METRIC_LEVEL = 'ALL';

ALTER SCHEMA tasty_bytes_dbt_db.prod SET LOG_LEVEL = 'INFO';
ALTER SCHEMA tasty_bytes_dbt_db.prod SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA tasty_bytes_dbt_db.prod SET METRIC_LEVEL = 'ALL';

CREATE OR REPLACE API INTEGRATION git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/')
  ENABLED = TRUE;

CREATE OR REPLACE FILE FORMAT tasty_bytes_dbt_db.public.csv_ff 
type = 'csv';

CREATE OR REPLACE STAGE tasty_bytes_dbt_db.public.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = tasty_bytes_dbt_db.public.csv_ff;

CREATE OR REPLACE STAGE tasty_bytes_dbt_db.public.internal_stage
FILE_FORMAT = tasty_bytes_dbt_db.public.csv_ff;

PUT file://local_path/*.csv @tasty_bytes_dbt_db.public.internal_stage;


/*--
 raw zone table build 
--*/

-- country table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.country
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
) 
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

INSERT INTO tasty_bytes_dbt_db.raw.country VALUES
(1, 'Japan', 'JPY', 'JP', 101, 'Tokyo', '14000000'),
(2, 'United States', 'USD', 'US', 201, 'New York', '8400000');


-- franchise table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.franchise 
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216) 
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

INSERT INTO tasty_bytes_dbt_db.raw.franchise VALUES
(1001, 'Taro', 'Yamada', 'Tokyo', 'Japan', 'taro@example.com', '090-0000-0000'),
(1002, 'John', 'Smith', 'New York', 'United States', 'john@example.com', '+1-212-000-0000');


-- location table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.location
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

INSERT INTO tasty_bytes_dbt_db.raw.location VALUES
(10, 'pk_tokyo_001', 'Shibuya Crossing', 'Tokyo', 'Kanto', 'JP', 'Japan'),
(20, 'pk_ny_001', 'Times Square', 'New York', 'NY', 'US', 'United States');


-- menu table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

INSERT INTO tasty_bytes_dbt_db.raw.menu
SELECT
  1, 10, 'Burgers', 'Tokyo Burger Truck',
  10001, 'Cheeseburger', 'Food', 'Burger',
  3.50, 8.00,
  PARSE_JSON('{"calories": 550, "protein": "25g"}')
UNION ALL
SELECT
  2, 20, 'Drinks', 'NY Drink Truck',
  20001, 'Cola', 'Drink', 'Soda',
  0.50, 3.00,
  PARSE_JSON('{"calories": 150, "sugar": "39g"}');




-- truck table build 
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

INSERT INTO tasty_bytes_dbt_db.raw.truck VALUES
(
  5001, 10, 'Tokyo', 'Kanto', 'JP-13', 'Japan', 'JP',
  1, 2022, 'Toyota', 'HiAce', 0, 1001, '2022-04-01'
),
(
  5002, 20, 'New York', 'NY', 'US-NY', 'United States', 'US',
  0, 2021, 'Ford', 'Transit', 1, NULL, '2021-06-15'
);




-- order_header table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

INSERT INTO tasty_bytes_dbt_db.raw.order_header VALUES
(
  90001, 5001, 10, 30001, NULL, 1,
  '09:00:00', '17:00:00', 'POS',
  '2024-01-10 12:30:00', '12:40',
  'JPY', 1500.00, '150', '0', 1650.00
),
(
  90002, 5002, 20, 30002, NULL, 2,
  '10:00:00', '18:00:00', 'ONLINE',
  '2024-01-11 13:00:00', '13:10',
  'USD', 12.00, '1.00', '0', 13.00
);



-- order_detail table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.order_detail 
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

INSERT INTO tasty_bytes_dbt_db.raw.order_detail VALUES
(1, 90001, 10001, NULL, 1, 2, 750.00, 1500.00, '0'),
(2, 90002, 20001, NULL, 1, 1, 12.00, 12.00, '0');



-- customer loyalty table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

INSERT INTO tasty_bytes_dbt_db.raw.customer_loyalty VALUES
(
  30001, 'Hanako', 'Suzuki', 'Tokyo', 'Japan', '150-0001',
  'JP', 'F', 'Tokyo Burger Truck', 'Single', '0',
  '2023-01-01', '1995-05-20', 'hanako@example.com', '080-1111-1111'
),
(
  30002, 'Emily', 'Brown', 'New York', 'United States', '10001',
  'EN', 'F', 'NY Drink Truck', 'Married', '1',
  '2022-06-15', '1990-08-10', 'emily@example.com', '+1-917-000-0000'
);








/*--
 raw zone table load 
--*/

-- country table load
COPY INTO tasty_bytes_dbt_db.raw.country
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/country/;

-- franchise table load
COPY INTO tasty_bytes_dbt_db.raw.franchise
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/franchise/;

-- location table load
COPY INTO tasty_bytes_dbt_db.raw.location
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/location/;

-- menu table load
COPY INTO tasty_bytes_dbt_db.raw.menu
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/menu/;

-- truck table load
COPY INTO tasty_bytes_dbt_db.raw.truck
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/truck/;

-- customer_loyalty table load
COPY INTO tasty_bytes_dbt_db.raw.customer_loyalty
FROM @tasty_bytes_dbt_db.public.s3load/raw_customer/customer_loyalty/;

-- order_header table load
COPY INTO tasty_bytes_dbt_db.raw.order_header
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/order_header/;

-- order_detail table load
COPY INTO tasty_bytes_dbt_db.raw.order_detail
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/order_detail/;

-- setup completion note
SELECT 'tasty_bytes_dbt_db setup is now complete' AS note;
