/*
Recast the datatpyes in the orders_table
*/

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(20),
ALTER COLUMN store_code TYPE VARCHAR(14),
ALTER COLUMN product_code TYPE VARCHAR(14),
ALTER COLUMN product_quantity TYPE SMALLINT;

/*
Recast the dtypes in the dim_users table
*/

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country_code TYPE VARCHAR(3),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE;

/*
Recast the dtypes in the dim_store_details table
*/

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(14),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(3),
ALTER COLUMN continent TYPE VARCHAR(255);


/*
Recast the dtypes in the dim_card_details table
*/
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5);

/*
Recast the dtypes in the dim_date_times table
*/
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(12),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

/*
Add a new column weight_class which will contain human-readable values based on the weight range of the product.

+--------------------------+-------------------+
| weight_class VARCHAR(?)  | weight range(kg)  |
+--------------------------+-------------------+
| Light                    | < 2               |
| Mid_Sized                | >= 2 - < 40       |
| Heavy                    | >= 40 - < 140     |
| Truck_Required           | => 140            |
+----------------------------+-----------------+
*/
--This Runs first to create the column needed
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(16);

--This Updates the column with the new values dependant on the weight_in_kg column values
UPDATE dim_products
SET weight_class = CASE 
		WHEN weight_in_kg < 2 THEN 'Light'
		WHEN weight_in_kg >= 2 AND weight_in_kg < 40 THEN 'Mid-Sized'
		WHEN weight_in_kg >= 40 AND weight_in_kg < 140 THEN 'Heavy'
		WHEN weight_in_kg >= 140 THEN 'Truck_Required'
	END;

-- Rename removed col
ALTER TABLE dim_products
RENAME removed TO still_available;

-- Recast column data types where needed
ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17),
ALTER COLUMN product_code TYPE VARCHAR(14),
ALTER COLUMN uuid TYPE uuid USING uuid::uuid;

-- update the values in the still_available col to True or False ready for Bool dtype change
UPDATE dim_products
SET still_available = CASE 
		WHEN still_available = 'Removed' THEN False
		ELSE True
	END;

-- Alter col to Boolean
ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOL USING still_available::boolean;

/*
Create primary keys
*/

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_time
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

/*
Create Foreign keys
*/

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date_times
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_products
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_details
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_users
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);
