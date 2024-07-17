SELECT * FROM dim_card_details;
SELECT * FROM dim_users;
SELECT * FROM dim_store_details;
SELECT * FROM dim_products;
SELECT * FROM orders_table;
SELECT * FROM dim_date_times;

DELETE FROM dim_users;
DROP TABLE dim_users CASCADE;
DROP TABLE dim_card_details CASCADE;
DROP TABLE dim_store_details CASCADE;
DROP TABLE dim_products CASCADE;
DROP TABLE orders_table CASCADE;

--orders_table
ALTER TABLE orders_table 
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;
ALTER TABLE orders_table
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid;
ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(20) USING card_number::VARCHAR;
ALTER TABLE orders_table
	ALTER COLUMN store_code TYPE VARCHAR(15) USING store_code::VARCHAR;
ALTER TABLE orders_table
	ALTER COLUMN product_code TYPE VARCHAR(15) USING product_code::VARCHAR;
ALTER TABLE orders_table
	ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT;
--dim_users
SELECT MAX(LENGTH(country_code)) AS max_len_country_code FROM dim_users;

ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::VARCHAR;
ALTER TABLE dim_users
	ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::VARCHAR;
ALTER TABLE dim_users
	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE;
ALTER TABLE dim_users
	ALTER COLUMN country_code TYPE VARCHAR(3) USING country_code::VARCHAR;
ALTER TABLE dim_users
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;
ALTER TABLE dim_users
	ALTER COLUMN join_date TYPE DATE USING join_date::DATE;
--store_details
UPDATE dim_store_details
	SET latitude = COALESCE(lat::TEXT,latitude::TEXT);

ALTER TABLE dim_store_details
	DROP COLUMN lat;
SELECT MAX(LENGTH(store_code)) AS max_len_store_code FROM dim_store_details;
SELECT MAX(LENGTH(country_code)) AS max_len_country_code FROM dim_store_details;

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT;
ALTER TABLE dim_store_details
	ALTER COLUMN locality TYPE VARCHAR(255) USING locality::VARCHAR;
ALTER TABLE dim_store_details
	ALTER COLUMN store_code TYPE VARCHAR(20) USING store_code::VARCHAR;
ALTER TABLE dim_store_details
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;
ALTER TABLE dim_store_details
	ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE;
ALTER TABLE dim_store_details
	ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::VARCHAR,
	ALTER COLUMN store_type DROP NOT NULL;
ALTER TABLE dim_store_details
	ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT;
ALTER TABLE dim_store_details
	ALTER COLUMN country_code TYPE VARCHAR(10) USING country_code::VARCHAR;
ALTER TABLE dim_store_details
	ALTER COLUMN continent TYPE VARCHAR(255) USING continent::VARCHAR;

--dim_products
SELECT * FROM dim_products;
--Update product_price values replcaing £ with an empty string
UPDATE dim_products
	SET product_price = REPLACE(product_price, '£', '')
	WHERE product_price LIKE '£%';

-- Add the new 'weight_class' column based on the weight ranges
ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products
    SET weight_class = CASE
        WHEN weight < 2 THEN 'Light'
        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
        ELSE 'Truck_Required'
    END; 

SELECT ean FROM dim_products;

 ALTER TABLE dim_products
    RENAME COLUMN "EAN" TO ean;
SELECT MAX(LENGTH(ean)) AS max_len_EAN FROM dim_products;
ALTER TABLE dim_products
    RENAME COLUMN ean TO "EAN";


SELECT MAX(LENGTH(weight_class)) AS max_weight_class FROM dim_products;
SELECT MAX(LENGTH(product_code)) AS max_product_code FROM dim_products;

 ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;
 ALTER TABLE dim_products
    ALTER COLUMN weight TYPE FLOAT;
 ALTER TABLE dim_products
    ALTER COLUMN EAN TYPE VARCHAR(17);
 ALTER TABLE dim_products
    ALTER COLUMN product_code TYPE VARCHAR(12);
 ALTER TABLE dim_products
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE;
 ALTER TABLE dim_products
    ALTER COLUMN uuid TYPE UUID USING uuid::UUID;	
 ALTER TABLE dim_products
    ALTER COLUMN weight_class TYPE VARCHAR(14);

--changing the datatype of removed to boolean
 ALTER TABLE dim_products
	ALTER COLUMN removed TYPE BOOL 
	 USING CASE 
	 WHEN removed = 'Still_avaliable' THEN true
	 WHEN removed = 'Removed' THEN false
	 ELSE NULL
 END;
--dim_date_times
SELECT * FROM dim_date_times;
SELECT MAX(LENGTH(time_period)) AS max_time_period FROM dim_date_times;
SELECT MAX(LENGTH(month)) AS max_len_month FROM dim_date_times;

ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2);

ALTER TABLE dim_date_times
	ALTER COLUMN year TYPE VARCHAR(4);
ALTER TABLE dim_date_times
	ALTER COLUMN day TYPE VARCHAR(2);
ALTER TABLE dim_date_times
	ALTER COLUMN time_period TYPE VARCHAR(10);
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID; 

--dim_card_details
SELECT * FROM dim_card_details;

SELECT MAX(LENGTH(card_number::TEXT)) AS max_card_length FROM dim_card_details;

SELECT MAX(LENGTH(expiry_date::TEXT)) AS max_date_length FROM dim_card_details;

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19);
ALTER TABLE dim_card_details
	ALTER COLUMN expiry_date TYPE VARCHAR(20);
ALTER TABLE dim_card_details
	ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;
-- ADDING PRIMARY KEYS 
--ADDING primary key in dim_card_details
ALTER TABLE dim_card_details 
	ADD PRIMARY KEY (card_number); 
--ADDING primary key in dim_date_times
ALTER TABLE dim_date_times
	ADD PRIMARY KEY (date_uuid); 
--ADDING primary key in dim_products
ALTER TABLE dim_products
	ADD PRIMARY KEY (product_code); 
--ADDING primary key in dim_store_details
ALTER TABLE dim_store_details
	ADD PRIMARY KEY (store_code); 
--ADDING primary key in dim_users
ALTER TABLE dim_users
	ADD PRIMARY KEY (user_uuid); 

-- Creating foreign keys in order_table
 ALTER TABLE orders_table
    ADD FOREIGN KEY (date_uuid) 
    	REFERENCES dim_date_times(date_uuid);

-- Alter orders_table date_uuid type to be compatible 
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

--dim_users foreign key 
ALTER TABLE orders_table 
    ADD FOREIGN KEY (user_uuid) 
   		 REFERENCES dim_users(user_uuid);
-- to identify missing user_uuid values
SELECT DISTINCT user_uuid
	FROM orders_table
		WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);
-- some uuid where not in dim_users uuid to match values to orders_table
--Alter user uuid in dim_users to be compatible with orders_table uuid
INSERT INTO dim_users (index, first_name, last_name, date_of_birth, company, email_address, address,country,country_code, phone_number, join_date, user_uuid)
	SELECT NULL, NULL, NULL, NULL, NULL, NULL,NULL,NULL,NULL,NULL,NULL,user_uuid
	FROM( 
		SELECT DISTINCT user_uuid
			FROM orders_table
			WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users)) AS missing_uuid;

ALTER TABLE orders_table 
    ADD FOREIGN KEY (card_number) 
    REFERENCES dim_card_details (card_number);
--incompatible datatypes for card_number in orders_table
ALTER TABLE orders_table
	ALTER COLUMN card_number TYPE VARCHAR(19);

SELECT DISTINCT card_number
FROM orders_table
WHERE card_number NOT IN(SELECT card_number FROM dim_card_details);

INSERT INTO dim_card_details(card_number, expiry_date, card_provider, date_payment_confirmed)
	SELECT card_number, NULL, NULL, NULL
        FROM (
            SELECT DISTINCT card_number
            FROM orders_table
            WHERE card_number NOT IN (SELECT card_number FROM dim_card_details)
        ) AS missing_cards_details;

ALTER TABLE orders_table 
    ADD FOREIGN KEY (store_code) 
    REFERENCES dim_store_details(store_code);

SELECT DISTINCT store_code
            FROM orders_table
            WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

INSERT INTO dim_store_details (index, address, longitude, latitude, locality, store_code, staff_numbers, opening_date, store_type, country_code, continent)
        SELECT NULL, NULL, NULL, NULL, NULL, store_code, NULL, NULL, NULL, NULL, NULL
        FROM (
            SELECT DISTINCT store_code
            FROM orders_table
            WHERE store_code NOT IN (SELECT store_code FROM dim_store_details)
        ) AS missing_store_code;

ALTER TABLE orders_table 
    ADD FOREIGN KEY (product_code) 
    REFERENCES dim_products(product_code);
