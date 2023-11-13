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

SELECT * FROM orders_table;