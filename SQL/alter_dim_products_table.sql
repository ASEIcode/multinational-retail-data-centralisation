SELECT * FROM dim_products

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

